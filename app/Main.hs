{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

module Main where

import Data.Proxy

import GHC.Generics
import GHC.OverloadedLabels
import GHC.TypeLits

import qualified Indexer as I

data Person = Person
  { name :: String
  , age :: Int
  , age1:: Int
  , age2:: Int
  , age3:: Int
  } deriving Generic

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

type family HasField (n :: Nat) (a :: k) (b :: [(k, *)]) :: Maybe (*, Nat) where
  HasField n a ('(a, t) : as) = Just '(t, n)
  HasField n a (b : as) = HasField (n + 1) a as
  HasField n _ '[] = Nothing

data Indexed (index :: [(Symbol, *)]) a = Indexed [Integer]
  deriving Show

data DB (index :: [(Symbol, *)]) a = DB
  deriving Show

empty :: Indexed '[] a
empty = Indexed []

hash 
  :: forall a s n b index list
  . KnownSymbol s
  => KnownNat n
  => GRowToList (Rep a) ~ list
  => HasField 0 s list ~ Just '(b, n)
  => Generic a
  => Field s
  -> Indexed index a
  -> Indexed ('(s, b) : index) a
hash _ (Indexed indexes) = Indexed (natVal (Proxy :: Proxy n):indexes)

load
 :: FilePath
 -> Indexed index a
 -> IO (DB index a)
load = undefined

person :: Indexed '[] Person
person = empty

hashes
  = hash #age3
  $ hash #age person

get
  :: KnownSymbol s
  => KnownNat n
  => HasField 0 s index ~ Just '(b, n)
  => Field s
  -> b
  -> Indexed index a -- DB
  -> [a]
get = undefined

test = get #age3 5 hashes

main :: IO ()
main = do
  indexer <- I.makeIndexes "cbits/scheduled_stops.csv" 9 [3] [1]
  result <- I.getLinesForIndex indexer 3 "5220WDB47866"
  print result
  result <- I.getLinesForSortedIndex indexer 1 "1"
  print result
