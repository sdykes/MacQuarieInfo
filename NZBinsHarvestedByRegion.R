library(tidyverse)

con <- DBI::dbConnect(odbc::odbc(),    
                      Driver = "ODBC Driver 18 for SQL Server", 
                      Server = "abcrepldb.database.windows.net",  
                      Database = "ABCPacker2023Repl",   
                      UID = "abcadmin",   
                      PWD = "Trauts2018!",
                      Port = 1433
)

BinsHarvestedByRegion2023 <- DBI::dbGetQuery(con,
                                             "SELECT 
	                                                st.Season,
	                                                ft.State,
	                                                SUM(bd.NoOfBins) AS NoOfBins
                                              FROM ma_Bin_DeliveryT AS bd
                                              INNER JOIN
	                                                (
	                                                SELECT
		                                                  SeasonID,
		                                                  SeasonDesc AS Season
	                                                FROM sw_SeasonT
	                                                ) AS st
                                              ON bd.SeasonID = st.SeasonID
                                              INNER JOIN
	                                                (
	                                                SELECT
		                                                FarmID,
		                                                State
	                                                FROM sw_FarmT
	                                                ) AS ft
                                              ON bd.FarmID = ft.FarmID 
                                              WHERE PresizeFlag = 0
                                              GROUP BY st.Season, ft.State"
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

BinsHarvestedByRegion2024 <- DBI::dbGetQuery(con,
                                             "SELECT 
	                                                st.Season,
	                                                ft.State,
	                                                SUM(bd.NoOfBins) AS NoOfBins
                                              FROM ma_Bin_DeliveryT AS bd
                                              INNER JOIN
	                                                (
	                                                SELECT
		                                                  SeasonID,
		                                                  SeasonDesc AS Season
	                                                FROM sw_SeasonT
	                                                ) AS st
                                              ON bd.SeasonID = st.SeasonID
                                              INNER JOIN
	                                                (
	                                                SELECT
		                                                FarmID,
		                                                State
	                                                FROM sw_FarmT
	                                                ) AS ft
                                              ON bd.FarmID = ft.FarmID 
                                              WHERE PresizeFlag = 0
                                              GROUP BY st.Season, ft.State"
)

DBI::dbDisconnect(con)

BinsHarvestedByRegion <- BinsHarvestedByRegion2023 |>
  bind_rows(BinsHarvestedByRegion2024) |>
  mutate(Season = as.integer(Season)) |>
  pivot_wider(id_cols = Season,
              names_from = State,
              values_from = NoOfBins,
              values_fill = 0) |>
  arrange(Season) |>
  rowwise() |>
  mutate(TotalBins = sum(c_across(!Season)))

write_csv(BinsHarvestedByRegion, "BinsHarvestedByRegion.csv")

