module Indexer where

import qualified Data.Aeson as A
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as BU

import qualified Data.HashMap.Strict as M
import qualified Data.Text as T

import qualified Data.Word as W

import Foreign.Marshal.Array
import Foreign.ForeignPtr
import Foreign.Ptr
import Foreign.Storable
import Foreign.C.Types
import Foreign.C.String

import System.IO.MMap
import System.IO.Unsafe (unsafePerformIO)

data Indexer'
type Indexer = ForeignPtr Indexer'

foreign import ccall unsafe "makeIndexes" c_makeIndexes
  :: CString
  -> CInt
  -> Ptr CInt
  -> CInt
  -> Ptr CInt
  -> CInt
  -> IO (Ptr Indexer')

foreign import ccall unsafe "&freeIndexes" c_freeIndexes
  :: FunPtr (Ptr Indexer' -> IO ())

foreign import ccall unsafe "getLinesForIndex" c_getLinesForIndex
  :: Ptr Indexer'
  -> CInt
  -> CString
  -> IO (Ptr CInt)

foreign import ccall unsafe "getLinesForSortedIndex" c_getLinesForSortedIndex
  :: Ptr Indexer'
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
  ptr <- withCString file $ \cfile -> do
    withArray (map toc indexes) $ \indexes' -> do
      withArray (map toc sortedIndexes) $ \sortedIndexes' -> do
        c_makeIndexes cfile (toc columnCount) indexes' (toc $ length indexes) sortedIndexes' (toc $ length sortedIndexes)
  newForeignPtr c_freeIndexes ptr

getLinesForIndex
  :: Indexer
  -> Int
  -> B.ByteString
  -> IO [[Int]]
getLinesForIndex indexer index value = withForeignPtr indexer $ \ptr -> do
  result <- B.useAsCString value $ \value' -> c_getLinesForIndex ptr (toc index) value'
  CInt length <- peek result
  array <- peekArray (fromIntegral length) result
  c_freeResult result
  pure $ chunksOf 2 $ map fromIntegral $ drop 1 array

getLinesForSortedIndex
  :: Indexer
  -> Int
  -> B.ByteString
  -> IO (Maybe (Int, Int))
getLinesForSortedIndex indexer index value = withForeignPtr indexer $ \ptr -> do
  result <- B.useAsCString value $ \value' -> c_getLinesForSortedIndex ptr (toc index) value'
  [offset, length] <- peekArray 2 result
  c_freeResult result
  if offset == CInt (-1)
    then pure Nothing
    else pure $ Just (fromIntegral offset, fromIntegral length)

chunksOf :: Int -> [e] -> [[e]]
chunksOf i ls = map (take i) (build (splitter ls)) where
  splitter :: [e] -> ([e] -> a -> a) -> a -> a
  splitter [] _ n = n
  splitter l c n  = l `c` splitter (drop i l) c n

  build :: ((a -> [a] -> [a]) -> [a] -> [a]) -> [a]
  build g = g (:) []

