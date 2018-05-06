{-# LANGUAGE ForeignFunctionInterface #-}

module Main where

import           Lib

import qualified Data.Aeson as A
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as BU

import qualified Data.HashMap.Strict as M
import qualified Data.Text as T

import qualified Data.Word as W
import qualified Data.Vector.Storable as V

import Foreign.ForeignPtr
import Foreign.Ptr
import Foreign.Storable
import Foreign.C.Types
import Foreign.C.String

import System.IO.MMap
import System.IO.Unsafe (unsafePerformIO)

type Indices = M.HashMap T.Text (V.Vector W.Word32)

main_ :: IO ()
main_ = do
  file <- B.readFile "../indices.json"
  case A.eitherDecodeStrict file of
    Left e -> error e
    Right m -> printSize m

  where
    printSize :: Indices -> IO ()
    printSize = print . M.size

--------------------------------------------------------------------------------

data API

foreign import ccall unsafe "makeIndices" c_makeIndices :: CString -> IO (Ptr API)

foreign import ccall unsafe "getLine" c_getLine :: Ptr API -> CString -> IO (Ptr CInt)

loadFile :: IO (B.ByteString, Ptr API)
loadFile = do
  buffer <- mmapFileByteString "/home/phil/data/out/scheduled_stops.csv" Nothing
  str <- newCString "/home/phil/data/out/scheduled_stops.csv"
  api <- c_makeIndices str
  pure (buffer, api)

query :: (B.ByteString, Ptr API) -> String -> IO ()
query (buffer, api) atcocode = do
  ac <- newCString atcocode
  array <- c_getLine api ac
  CInt length <- peek array
  fptr <- newForeignPtr_ array
  let v = V.unsafeFromForeignPtr0 fptr (fromIntegral length)

  mapM_ print (ranges buffer (chunksOf 2 $ drop 1 $ V.toList v))

main :: IO ()
main = do
  buffer <- mmapFileByteString "/home/phil/data/out/scheduled_stops.csv" Nothing

  str <- newCString "/home/phil/data/out/scheduled_stops.csv"
  api <- c_makeIndices str
  ac <- newCString "5220WDB47866"
  array <- c_getLine api ac
  CInt length <- peek array
  fptr <- newForeignPtr_ array
  let v = V.unsafeFromForeignPtr0 fptr (fromIntegral length)

  mapM_ print (ranges buffer (chunksOf 2 $ drop 1 $ V.toList v))
  -- print v

ranges bs rs =
  [ BU.unsafeTake (fromIntegral length) $ BU.unsafeDrop (fromIntegral start) bs
  | [CInt start, CInt length] <- rs
  ]

build :: ((a -> [a] -> [a]) -> [a] -> [a]) -> [a]
build g = g (:) []

chunksOf :: Int -> [e] -> [[e]]
chunksOf i ls = map (take i) (build (splitter ls)) where
  splitter :: [e] -> ([e] -> a -> a) -> a -> a
  splitter [] _ n = n
  splitter l c n  = l `c` splitter (drop i l) c n

