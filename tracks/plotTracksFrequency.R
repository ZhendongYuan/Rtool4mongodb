library("mongolite")
library("ggplot2")
# this code is plot the tracks' time
my_collection = mongo(collection = "indonesiaTracks15", db = "endoTracksWorld15")
result=my_collection$aggregate('[
  {
    "$project": {
      "date": {
        "$dateFromString": {
          "dateString": "$start_time",
          "format": "%Y-%m-%d %H:%M:%S"
        }
      }
    }
  },
  {
    "$project": {
      "y": {
        "$year": "$date"
      },
      "m": {
        "$month": "$date"
      },
      "d": {
        "$dayOfMonth": "$date"
      }
    }
  },
  {
    "$group": {
      "_id": {
        "year": "$y",
        "month": "$m",
        "day": "$d"
      },
      "count": {
        "$sum": 1
      }
    }
  },
  {
    "$sort": {
      "_id.year": 1,
      "_id.month": 1,
      "_id.day": 1
    }
  }
  
  ]',options='{
  "allowDiskUse": true
  }')

#save(result,file = "gloablpart5_daily_plot.RData")


rt = result
rt$time = paste(rt$`_id`$year,rt$`_id`$month,rt$`_id`$day,sep = "-")
rt$time = as.Date( rt$time , '%Y-%m-%d')

rt3 = rt

ggplot( data = rt, aes( time, count )) + geom_line() 
ggsave("NLX3P1_daily_plot.png")
