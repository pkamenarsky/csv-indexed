{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

module Csv.Indexed where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
 
import qualified Data.Csv as Csv
import qualified Data.LruCache.IO as LRU
import qualified Data.Vector as V

import Data.Monoid ((<>))
import Data.Proxy

import GHC.Generics
import GHC.OverloadedLabels
import GHC.TypeLits

import qualified Indexer as I
import           Unsafe.Coerce (unsafeCoerce)
import           System.IO.Unsafe (unsafeDupablePerformIO)

import Prelude hiding (lookup, readFile)

data Field (a :: Symbol) = Field

instance l ~ l' => IsLabel (l :: Symbol) (Field l') where
  fromLabel = Field

--------------------------------------------------------------------------------

type family GRowToList (r :: * -> *) :: [(Symbol, *)] where
  GRowToList (l :*: r)
    = GRowToList l ++ GRowToList r
  GRowToList (S1 ('MetaSel ('Just name) _ _ _) (Rec0 a))
    = '[ '(name, a) ]
  GRowToList (M1 _ m a)
    = GRowToList a
  GRowToList U1 = '[]

type family (a :: [k]) ++ (b :: [k]) :: [k] where
  '[] ++ bs = bs
  (a ': as) ++ bs = a ': (as ++ bs)

type family Length (a :: [k]) :: Nat where 
  Length (a : as) = 1 + Length as
  Length '[] = 0

type family HasField (n :: Nat) (a :: k) (b :: [(k, *)]) :: Maybe (*, Nat) where
  HasField n a ('(a, t) : as) = Just '(t, n)
  HasField n a (b : as) = HasField (n + 1) a as
  HasField n _ '[] = Nothing

data Indexes (index :: [(Symbol, *)]) a = Indexes [Int] [Int]
  deriving Show

unindexed :: Indexes '[] a
unindexed = Indexes [] []

data DB (index :: [(Symbol, *)]) a = DB I.Indexer (LRU.LruHandle B.ByteString ())

index 
  :: forall a s n b index list
  . KnownSymbol s
  => KnownNat n
  => GRowToList (Rep a) ~ list
  => HasField 0 s list ~ Just '(b, n)
  => Generic a
  => Field s
  -> Indexes index a
  -> Indexes ('(s, b) : index) a
index _ (Indexes indexes sortedIndexes)
  = Indexes (n:indexes) sortedIndexes
  where
    n = fromIntegral (natVal (Proxy :: Proxy n))

sorted 
  :: forall a s n b index list
  . KnownSymbol s
  => KnownNat n
  => GRowToList (Rep a) ~ list
  => HasField 0 s list ~ Just '(b, n)
  => Generic a
  => Field s
  -> Indexes index a
  -> Indexes ('(s, b) : index) a
sorted _ (Indexes indexes sortedIndexes)
  = Indexes indexes (n:sortedIndexes)
  where
    n = fromIntegral (natVal (Proxy :: Proxy n))

readFile
 :: forall a n index list
 . KnownNat n
 => GRowToList (Rep a) ~ list
 => Length list ~ n
 => FilePath
 -> Int
 -> Indexes index a
 -> IO (DB index a)
readFile path items (Indexes indexes sortedIndexes) =
  DB <$> I.makeIndexes path n indexes sortedIndexes <*> LRU.newLruHandle items
  where
    n = fromIntegral (natVal (Proxy :: Proxy n))

{-# NOINLINE lookupWith #-}
lookupWith
  :: forall a b n s field index list
  . KnownSymbol s
  => KnownNat n
  => GRowToList (Rep a) ~ list
  => HasField 0 s list ~ Just '(b, n)
  => HasField 0 s index ~ Just field
  => Generic a
  => Csv.FromRecord a
  => Csv.ToField b
  => Csv.DecodeOptions
  -> Field s
  -> b
  -> DB index a
  -> Either String [a]
lookupWith options _ value (DB indexer cache) = unsafeDupablePerformIO $ do
  result <- unsafeCoerce $ LRU.cached cache (BC.pack (show n) <> ":" <> Csv.toField value) $ unsafeCoerce (I.getRecordsForIndex options indexer n (Csv.toField value) :: IO (Either String (V.Vector a)))
  pure $ V.toList <$> result
  where
    n = fromIntegral (natVal (Proxy :: Proxy n))

{-# NOINLINE lookup #-}
lookup
  :: forall a b n s field index list
  . KnownSymbol s
  => KnownNat n
  => GRowToList (Rep a) ~ list
  => HasField 0 s list ~ Just '(b, n)
  => HasField 0 s index ~ Just field
  => Generic a
  => Csv.FromRecord a
  => Csv.ToField b
  => Field s
  -> b
  -> DB index a
  -> Either String [a]
lookup field value db = lookupWith Csv.defaultDecodeOptions field value db
