SELECT 
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
		FarmCode,
		FarmName,
		State
	FROM sw_FarmT
	) AS ft
ON bd.FarmID = ft.FarmID 
WHERE PresizeFlag = 0
GROUP BY st.Season, ft.State

