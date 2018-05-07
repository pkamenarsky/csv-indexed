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

module Main where

import qualified Data.Csv as Csv
import qualified Data.Vector as V

import GHC.Generics
import GHC.OverloadedLabels
import GHC.TypeLits

import Csv.Indexed

import qualified Indexer as I
import           System.IO.Unsafe (unsafePerformIO)
import           System.Random (randomRIO)

import Prelude hiding (lookup, readFile)

data ScheduledStop = ScheduledStop
  { ssId :: Int
  , ssBusScheduledJourneyCode :: Int
  , ssScheduledStopType :: String
  , ssBusStoppointAtcocode :: String
  , ssPublicArrival :: Maybe String
  , ssPublicDeparture :: Maybe String
  , ssOrderNo :: Int
  , ssCreatedAt :: String
  , ssUpdatedAt :: String
  } deriving (Eq, Generic, Csv.FromRecord, Show)

type ScheduledStopIndexes = 
  [ '("ssBusStoppointAtcocode",    String)
  , '("ssBusScheduledJourneyCode", Int)
  ]

scheduledStopIndexes :: Indexes ScheduledStopIndexes ScheduledStop
scheduledStopIndexes
  = index  #ssBusStoppointAtcocode
  $ sorted #ssBusScheduledJourneyCode
    unindexed

randomReqs :: DB ScheduledStopIndexes ScheduledStop -> Int -> Int -> IO Int
randomReqs db 0 acc = pure acc
randomReqs db n acc = do
  x <- randomRIO (1, 200000)
  case lookup #ssBusScheduledJourneyCode x db of
    Right result -> randomReqs db (n - 1) (acc + sum (map ssOrderNo result))
    Left e -> error e

main :: IO ()
main = do
  db <- readFile "/home/phil/data/20180418/out/tapi/scheduled_stops.csv" scheduledStopIndexes

  let result1 = lookup #ssBusStoppointAtcocode "5220WDB47866" db
      result2 = lookup #ssBusScheduledJourneyCode 2 db

  print result1
  print result2
