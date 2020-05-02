import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrameStatFunctions

        val ss = SparkSession.builder.appName("WeatherAnalytic").getOrCreate()
        //load in weather.csv
        var weather_df = ss.read.format("csv").option("header", "true").load("hdfs:///user/hbp253/Weather.csv")
        //analyze the day to day temperature & humidity change for avg_temp for seoul province
        var seoul_weather_df = weather_df.filter($"province" === "Seoul")
        //should probably partition the data better so there are more partitions that can be parallelized
        val windowSpec = Window.partitionByorderBy($"date")
        seoul_weather_df = seoul_weather_df.withColumn("temp_change", ( $"avg_temp" - lag($"avg_temp", 1).over(windowSpec) ) )
        seoul_weather_df = seoul_weather_df.withColumn("hum_change",lag($"avg_relative_humidity", 1).over(windowSpec))
        //load in PatientInfo
        val patient_df = ss.read.format("csv").option("header", "true").load("hdfs:///user/hbp253/PatientInfo.csv")
        //filter to only view patients from Seoul Province and that is not coming in infected from other countries
        var seoul_patient_df = patient_df.filter(($"province" === "Seoul") && ($"infection_case" !== "overseas inflow"))
        //on the side, find the avg number of days between symptom_onset(when row has it) - confirmed_date
        val seoul_patient_df_withonset = seoul_patient_df.filter($"symptom_onset_date" !== "null")
        val seoul_patient_df_withdelay = seoul_patient_df_withonset.withColumn("symptom_confirm_delay", datediff($"confirmed_date", $"symptom_onset_date"))
        val avg_symptom_confirm_delay = seoul_patient_df_withdelay.select(avg($"symptom_confirm_delay")).first().getDouble(0)
        //use confirmed date (when test result came back positive) 
        //and take it back 6 days (avg. incubation period)
        //for rows that doesn't contain the symptom_onset_date, use avg. delay btw symptom and confirm date to estimate date of contraction 
        //referenced incubation period: https://www.nejm.org/doi/full/10.1056/NEJMoa2001316 
        seoul_patient_df = seoul_patient_df.withColumn("estimate_contraction_date", when($"symptom_onset_date" !== "null", date_sub($"symptom_onset_date", 6)).otherwise(date_sub($"confirmed_date", (6 + (avg_symptom_confirm_delay).toInt)) ) )
        //group the estimate_contraction_date by date and agg. the count of infection for that day
        var contraction_count_df = seoul_patient_df.groupBy("estimate_contraction_date").count()
        contraction_count_df = contraction_count_df.withColumnRenamed("estimate_contraction_date", "date")
        // left join the values of the 
        var weather_contraction_df = seoul_weather_df.join(contraction_count_df, Seq("date"), "left")
        weather_contraction_df = weather_contraction_df.na.fill(0, Seq("count"))
        weather_contraction_df = weather_contraction_df.withColumn("count", col("count").cast("Double"))
        //Get the first date that the count is not 0
        weather_contraction_df.filter($"count" !== 0).sort("date").first
        val start_contraction_df = weather_contraction_df.filter($"date" >= "2020-01-20")
        //Calc. Pearson correlation btw change in temperature and change in contraction
        weather_contraction_df.stat.corr("temp_change", "count")
        //= 0.014898955230651578
        //only start counting the correlation beginning from the start of contractions
        start_contraction_df.stat.corr("temp_change", "count")
        //= 0.04768061414237815