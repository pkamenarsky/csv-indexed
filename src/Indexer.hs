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

data CIndexer
data Indexer = Indexer B.ByteString (ForeignPtr CIndexer)

foreign import ccall unsafe "makeIndexes" c_makeIndexes
  :: CString
  -> W.Word32
  -> Ptr W.Word32
  -> W.Word32
  -> Ptr W.Word32
  -> W.Word32
  -> IO (Ptr CIndexer)

foreign import ccall unsafe "&freeIndexes" c_freeIndexes
  :: FunPtr (Ptr CIndexer -> IO ())

foreign import ccall unsafe "getLinesForIndex" c_getLinesForIndex
  :: Ptr CIndexer
  -> W.Word32
  -> CString
  -> IO (Ptr W.Word64)

foreign import ccall unsafe "freeResult" c_freeResult
  :: Ptr W.Word64
  -> IO ()

fi :: (Integral a, Num b) => a -> b 
fi = fromIntegral

makeIndexes
  :: FilePath
  -> Int
  -> [Int]
  -> [Int]
  -> IO Indexer
makeIndexes file columnCount indexes sortedIndexes = do
  buffer <- mmapFileByteString file Nothing

  ptr <- withCString file $ \cfile -> do
    withArray (map fi indexes) $ \indexes' -> do
      withArray (map fi sortedIndexes) $ \sortedIndexes' -> do
        c_makeIndexes cfile (fi columnCount) indexes' (fi $ length indexes) sortedIndexes' (fi $ length sortedIndexes)
  Indexer <$> pure buffer <*> newForeignPtr c_freeIndexes ptr

getLinesForIndex
  :: Indexer
  -> Int
  -> B.ByteString
  -> IO [[W.Word64]]
getLinesForIndex (Indexer _ indexer) index value = withForeignPtr indexer $ \ptr -> do
  result <- B.useAsCString value $ \value' -> c_getLinesForIndex ptr (fi index) value'
  length <- peek result
  array <- peekArray (fi length) result
  c_freeResult result
  pure $ chunksOf 2 $ drop 1 array

ranges :: Csv.FromRecord a
  => Csv.DecodeOptions
  -> B.ByteString
  -> [[W.Word64]]
  -> Either String (V.Vector a)
ranges options bs rs = mconcat <$> sequence
  [ Csv.decodeWith options Csv.NoHeader
    $ BL.fromStrict
    $ BU.unsafeTake (fi length)
    $ BU.unsafeDrop (fi start) bs
  | [start, length] <- rs
  ]

getRecordsForIndex
  :: Csv.FromRecord a
  => Csv.DecodeOptions
  -> Indexer
  -> Int
  -> B.ByteString
  -> IO (Either String (V.Vector a))
getRecordsForIndex options indexer@(Indexer buffer _) index value = do
  ranges options buffer <$> getLinesForIndex indexer index value

chunksOf :: Int -> [e] -> [[e]]
chunksOf i ls = map (take i) (build (splitter ls)) where
  splitter :: [e] -> ([e] -> a -> a) -> a -> a
  splitter [] _ n = n
  splitter l c n  = l `c` splitter (drop i l) c n

  build :: ((a -> [a] -> [a]) -> [a] -> [a]) -> [a]
  build g = g (:) []

