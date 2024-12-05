library(tidyverse)

con <- DBI::dbConnect(odbc::odbc(), 
                      dsn = "RockIt", 
                      uid="stuart.dykes@rockitapple.com", 
                      authenticator = "externalbrowser"
)


SizeBandsBySeason <- DBI::dbGetQuery(con,
                             "SELECT 
                                  fd.SEASON,
                                  fd.GROWER_CODE,
                                  fd.SIZE_NAME,
                                  COUNT(fd.SIZER_GRADE_NAME) AS NUMBER_OF_APPLES
                              FROM
                                  (
                                  SELECT
                                    BATCH_ID,
                                    GROWER_CODE,
                                    SIZE_NAME,
                                    SIZER_GRADE_NAME,
                                    CASE
                                      WHEN START_TIME >= '2021-01-01 00:00:00.000' AND START_TIME < '2022-01-01 00:00:00.000' THEN 2021
                                      WHEN START_TIME >= '2022-01-01 00:00:00.000' AND START_TIME < '2023-01-01 00:00:00.000' THEN 2022
                                      WHEN START_TIME >= '2023-01-01 00:00:00.000' AND START_TIME < '2024-01-01 00:00:00.000' THEN 2023
                                      WHEN START_TIME >= '2024-01-01 00:00:00.000' AND START_TIME < '2025-01-01 00:00:00.000' THEN 2024
                                      ELSE 2025 
                                    END AS SEASON
                                  FROM ROCKIT_DATA_PROD.COMPAC.STG_COMPAC_BATCH
                                  WHERE SIZER_GRADE_NAME NOT IN ('Recycle','Low','Class 1.5','Leaf','Juic','Capture','Reject/Spoilt','Doub','1.5','Juice','Doubles','Low Colour','Capt','Capture')
                                  AND SIZER_GRADE_NAME IS NOT NULL
                                  AND SIZE_NAME NOT IN ('test','Rejects','5','Reje','reje','53/6','fami','US','OS','72/2')
                                  AND SIZE_NAME IS NOT NULL
                                  AND GROWER_CODE NOT IN ('','CSE')
                                  AND GROWER_CODE IS NOT NULL
                                  ) AS fd
                              GROUP BY fd.SEASON, fd.GROWER_CODE, fd.SIZE_NAME
                              ORDER BY SEASON, SIZE_NAME"
                              )

DBI::dbDisconnect(con)

con <- DBI::dbConnect(odbc::odbc(),    
                      Driver = "ODBC Driver 18 for SQL Server", 
                      Server = "abcrepldb.database.windows.net",  
                      Database = "ABCPacker2023Repl",   
                      UID = "abcadmin",   
                      PWD = "Trauts2018!",
                      Port = 1433
)

FarmMapping2023 <- DBI::dbGetQuery(con,
                                   "SELECT 
                                  FarmCode,
                                  State
                                FROM sw_FarmT"
)

DBI::dbDisconnect(con)

con <- DBI::dbConnect(odbc::odbc(),    
                      Driver = "ODBC Driver 18 for SQL Server", 
                      Server = "abcrepldb.database.windows.net",  
                      Database = "ABCPackerRepl",   
                      UID = "abcadmin",   
                      PWD = "Trauts2018!",
                      Port = 1433
)

FarmMapping2024 <- DBI::dbGetQuery(con,
                               "SELECT 
                                  FarmCode,
                                  State
                                FROM sw_FarmT"
                               )

DBI::dbDisconnect(con)
#
# Aggregate the old and new orchards and their regions
#
FarmMapping <- FarmMapping2023 |>
  full_join(FarmMapping2024, by = "FarmCode") |>
  mutate(State = coalesce(State.x, State.y)) |>
  select(-c(State.x, State.y)) |>
  mutate(State = if_else(State == "Hawkes bay", "Hawkes Bay", State)) |>
  filter(!is.na(State))

FarmMapping |> distinct(State)



SizeBandBySeasonFruitNo <- SizeBandsBySeason |>
  rename(FarmCode = GROWER_CODE) |>
  mutate(SizeBand = case_when(SIZE_NAME %in% c("53/2", "53/5") ~ 53,
                              SIZE_NAME == "58/5" ~ 58,
                              SIZE_NAME %in% c("63/3","63/4","63/5") ~ 63,
                              SIZE_NAME == "67/4" ~ 67,
                              SIZE_NAME == "72/4" ~ 72)) |>
  left_join(FarmMapping, by = "FarmCode") |>
  group_by(SEASON, SizeBand) |>
  summarise(FruitNumbers = sum(NUMBER_OF_APPLES, na.rm=T),
            .groups = "drop") |>
  pivot_wider(id_cols = SizeBand,
              names_from = SEASON,
              values_from = FruitNumbers) |>
  select(-"2025") 

write_csv(SizeBandBySeasonFruitNo, "SizeBandBySeasonFruitNo.csv")
  

SizeBandBySeasonProp <- SizeBandBySeasonFruitNo |>
  mutate(across(.cols = !SizeBand, ~./sum(.)))
  
write_csv(SizeBandBySeasonProp, "SizeBandBySeasonProp.csv")    

  

SizeBandsBySeason |> distinct(SIZE_NAME)



