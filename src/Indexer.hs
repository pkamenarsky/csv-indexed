module Indexer where

import qualified Data.Aeson as A
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Unsafe as BU
import qualified Data.Csv as Csv

import qualified Data.HashMap.Strict as M
import qualified Data.Text as T

import qualified Data.Word as W
import qualified Data.Vector as V

import           Foreign.Marshal.Array
import           Foreign.ForeignPtr
import           Foreign.Ptr
import           Foreign.Storable
import           Foreign.C.Types
import           Foreign.C.String

import           System.IO.MMap
import           System.IO.Unsafe (unsafePerformIO)

data CIndexer
data Indexer = Indexer B.ByteString (ForeignPtr CIndexer)

foreign import ccall unsafe "makeIndexes" c_makeIndexes
  :: CString
  -> CInt
  -> Ptr CInt
  -> CInt
  -> Ptr CInt
  -> CInt
  -> IO (Ptr CIndexer)

foreign import ccall unsafe "&freeIndexes" c_freeIndexes
  :: FunPtr (Ptr CIndexer -> IO ())

foreign import ccall unsafe "getLinesForIndex" c_getLinesForIndex
  :: Ptr CIndexer
  -> CInt
  -> CString
  -> IO (Ptr CInt)

foreign import ccall unsafe "freeResult" c_freeResult
  :: Ptr CInt
  -> IO ()

toc :: Int -> CInt
toc = CInt . fromIntegral

makeIndexes
  :: FilePath
  -> Int
  -> [Int]
  -> [Int]
  -> IO Indexer
makeIndexes file columnCount indexes sortedIndexes = do
  buffer <- mmapFileByteString file Nothing

  ptr <- withCString file $ \cfile -> do
    withArray (map toc indexes) $ \indexes' -> do
      withArray (map toc sortedIndexes) $ \sortedIndexes' -> do
        c_makeIndexes cfile (toc columnCount) indexes' (toc $ length indexes) sortedIndexes' (toc $ length sortedIndexes)
  Indexer <$> pure buffer <*> newForeignPtr c_freeIndexes ptr

getLinesForIndex
  :: Indexer
  -> Int
  -> B.ByteString
  -> IO [[Int]]
getLinesForIndex (Indexer _ indexer) index value = withForeignPtr indexer $ \ptr -> do
  result <- B.useAsCString value $ \value' -> c_getLinesForIndex ptr (toc index) value'
  CInt length <- peek result
  array <- peekArray (fromIntegral length) result
  c_freeResult result
  pure $ chunksOf 2 $ map fromIntegral $ drop 1 array

ranges :: Csv.FromRecord a
  => Csv.DecodeOptions
  -> B.ByteString
  -> [[Int]]
  -> Either String (V.Vector a)
ranges options bs rs = mconcat <$> sequence
  [ Csv.decodeWith options Csv.NoHeader
    $ BL.fromStrict
    $ BU.unsafeTake length
    $ BU.unsafeDrop start bs
  | [start, length] <- rs
  ]

getRecordsForIndex
  :: Csv.FromRecord a
  => Csv.DecodeOptions
  -> Indexer
  -> Int
  -> B.ByteString
  -> IO (Either String (V.Vector a))
getRecordsForIndex options indexer@(Indexer buffer _) index value =
  ranges options buffer <$> getLinesForIndex indexer index value

chunksOf :: Int -> [e] -> [[e]]
chunksOf i ls = map (take i) (build (splitter ls)) where
  splitter :: [e] -> ([e] -> a -> a) -> a -> a
  splitter [] _ n = n
  splitter l c n  = l `c` splitter (drop i l) c n

  build :: ((a -> [a] -> [a]) -> [a] -> [a]) -> [a]
  build g = g (:) []

