library("mongolite")
library("sf")
require("rgdal")
library("plyr")
library("data.table")
library("rpostgis")
track_collection = mongo(collection = "GBendoTracks", db = "GBendo")
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
starttime = Sys.time()

filter2points = function (mx)
{
  flagcursor <<- flagcursor +1
  #print(flagcursor)
  if (length(mx@Lines[[1]]@coords)>4)
  {
    #print(flagcursor)
    invalidTracksIndex <<- rbind(invalidTracksIndex,flagcursor)
  }
  
}
insertpostgis = function (linestrings,other_data)
{
  
  sl = SpatialLines(linestrings,proj4string = CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"))
  # handle heart rate
  temp_df = rbindlist(other_data, use.names=TRUE, fill=TRUE)
  sl_df = SpatialLinesDataFrame(sl,data=temp_df,match.ID = FALSE)
  # filter out only one point tracks
  flagcursor<<-0
  
  
  tempNull = lapply(sl_df@lines,function(x) filter2points(x))
  tempNull = NULL
  sl_df = sl_df[invalidTracksIndex[,1],]
  
  
  conn <- dbConnect(drv = "PostgreSQL", host = "localhost",port = "5432", dbname = "osm_uk", user = "zhendong", password = "")
  
  pgInsert(conn, "Englandtracks", sl_df, new.id = "gid")
  dbDisconnect(conn)
}


try(
  while(!is.null(tx <- it$batch(100))){
    group_df = lapply(tx,function(x) convert2linestring(x))
    group_df_line = lapply(group_df,function(x) x$line)
    linestrings = c(group_df_line,linestrings)
    group_df_data = lapply(group_df,function(x) x$data)
    other_data = c(group_df_data,other_data)
    count = count+1
    print(paste("how many time? *100 batch size:",count))
    #479369
    if(count %% 500 == 0 || count == 4793)
    {
      invalidTracksIndex = NULL
      insertpostgis(linestrings,other_data)
      linestrings = NULL
      other_data = NULL
      print(paste("finished: ",count))
    }
    
  }
)
print("funished batch")



print("end at: ")
print(Sys.time())
print("sart at:")
print(starttime)
#writeOGR(sl_df, dsn="GBendoTracksJan2Mar2015", layer="GBTracksJan2Mar2015", driver="ESRI Shapefile")



