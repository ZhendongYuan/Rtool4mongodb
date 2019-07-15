library("mongolite")
library("sf")
require("rgdal")
library("mapview")
library("plyr")
setwd("D:/endoGPS27/Indonesia")
track_collection = mongo(collection = "indonesiaTracks15", db = "endoTracksWorld15")
it =track_collection$aggregate('[{
	"$match": {
		"track_points": {
			"$exists": true
		}
	}
}]',
                              options='{"allowDiskUse": true}',
                              iterate = TRUE)
linestrings = NULL
other_data = NULL
convert2point = function (ax)
{
  point = st_point(c(ax$longitude,ax$latitude))
}
convert2linestring = function (mx)
{
  l = Lines(Line(t(sapply(mx$track_points,function(x) convert2point(x)))),ID =mx$workout_id)
  others =mx
  others$track_points <- NULL
  others$min_point <- NULL
  others$max_point <- NULL
  if (!is.null(others[["heart_rate_zones"]]))
  {
    others$heart_rate_zones = toString(others$heart_rate_zones)
  }
  return(list("line" = l, "data" = others))
}
count = 0
try(
while(!is.null(tx <- it$batch(100))){
  group_df = lapply(tx,function(x) convert2linestring(x))
  group_df_line = lapply(group_df,function(x) x$line)
  linestrings = c(group_df_line,linestrings)
  group_df_data = lapply(group_df,function(x) x$data)
  other_data = c(group_df_data,other_data)
  print("how many time? *100 batch size")
  count = count+1
  print(count)
}
)
sl = SpatialLines(linestrings,proj4string = CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"))
# handle heart rate

library("data.table")
temp_df = rbindlist(other_data, use.names=TRUE, fill=TRUE)
sl_df = SpatialLinesDataFrame(sl,data=temp_df,match.ID = FALSE)

writeOGR(sl_df, dsn=".", layer="IndoTracks15", driver="ESRI Shapefile")



