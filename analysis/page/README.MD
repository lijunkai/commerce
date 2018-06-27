### 页面跳转率: page2_page3 / lastPageCount(page1_page2)
* init config zip [page1_page2,page2_page4]
    * get to config param
    * scala zip 
* min granularity page1_page2
    * [sessionId,(page,startTime)]
    * groupByKey [sessionId,[(page,startTime)]]
    * flatMap [page1_page2,1] 
        * [(page,startTime)] scala list sort by startTime ASC
        * scala zip [page1_page2,page2_page3]
        * scala map [page1_page2,1] 
    * action: countByKey ([page1_page2,100],[page1_page2,100])
    * compute page conversion rate [("page1_page2=80%"),("page2_page3=60%")]     
        * get lastPageCount
        * foreach countByKey results, result / lastPageCount = pageConversionRate
    * conversion PageSplitConvertRate(taskUUID, rateStr)
        * compute results makeString("|")
    * write to mysql