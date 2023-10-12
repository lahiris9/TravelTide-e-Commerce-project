------------------------------------------------------
-- SQL to fetch data for analysing TravelTide customer behaviour
-- to create segmentation for marketing team
------------------------------------------------------
WITH 
/* CTE query to find hotel discount proportion
*/
cohort_disc_hotel_prop AS (
SELECT
      trip_id,
      -- to calculate the percentage of hotel bookings under discount
			SUM(CASE WHEN hotel_discount = 'true' THEN 1 ELSE 0 END) :: FLOAT * 100/ COUNT(*) AS discount_hotel_proportion
FROM flights f
     LEFT JOIN sessions s USING (trip_id)
     LEFT JOIN users u USING (user_id)
WHERE 
	   s.hotel_discount is not null
GROUP BY
      trip_id
),

cohort_disc_flight_prop AS (
SELECT
      trip_id,
      -- to calculate the percentage of flight bookings under discount
			SUM(CASE WHEN flight_discount = 'true' THEN 1 ELSE 0 END) :: FLOAT * 100/ COUNT(*) AS discount_flight_proportion
      
FROM flights f
     LEFT JOIN sessions s USING (trip_id)
     LEFT JOIN users u USING (user_id)
WHERE 
	   s.flight_discount is not null
GROUP BY
      trip_id
  ),

cohort_bag AS (
SELECT 
  		 trip_id,
       checked_bags AS bags -- number of bags used by customers per trip
FROM
      flights f
LEFT JOIN sessions s USING (trip_id)
LEFT JOIN users u USING (user_id)
GROUP BY
      trip_id
),

cohort_trip_dur AS (
      SELECT
      trip_id,
  		-- the difference between the return_time & departure_time give the duration for which the user was travelling,
  		-- till returning back to the start of the journey
      EXTRACT(DAY FROM (return_time - departure_time)) AS trip_dur 
    FROM
      flights f
      LEFT JOIN sessions s USING (trip_id)
      LEFT JOIN users u USING (user_id)
    WHERE
      s.flight_discount_amount is not null
    GROUP BY
      trip_id
),

/* CTE to find discounted amount saved by a customer each night stay in a hotel */
  cohort_ads_per_night AS (
    SELECT
      trip_id,
      SUM(s.hotel_discount_amount * hotel_per_room_usd) / SUM(h.nights) AS ADS_hotel
    FROM
      hotels h
      LEFT JOIN sessions s USING (trip_id)
      LEFT JOIN users u USING (user_id)
    WHERE
      s.hotel_discount_amount is not null
      AND h.nights > 0
    GROUP BY
      trip_id
  ),
  
/* CTE to find discounted amount saved by a customer for each actual kilometer (using the Haversine Distance function) they travelled in each flight trip*/  
  cohort_ads AS (
    SELECT
      trip_id,
      SUM(s.flight_discount_amount * base_fare_usd) / SUM(
        haversine_distance (
          u.home_airport_lat,
          u.home_airport_lon,
          f.destination_airport_lat,
          f.destination_airport_lon
        )
      ) AS ADS
    FROM
      flights f
      LEFT JOIN sessions s USING (trip_id)
      LEFT JOIN users u USING (user_id)
    WHERE
      s.flight_discount_amount is not null
      
    GROUP BY
      trip_id
  ),
/* Main CTE to find 10 specific aggregated values for selected TravelTide Customer data which can be used as Metrics
for performing customer segmentation through further statistical analysis*/  
  cohort_aggr AS (
    WITH
      cohort_filter AS (
        SELECT
          sessions.session_id,
          sessions.user_id,
          sessions.trip_id,
          origin_airport,
          destination,
          destination_airport,
          seats,
          return_flight_booked,
          departure_time,
          return_time,
          checked_bags,
          trip_airline,
          destination_airport_lat,
          destination_airport_lon,
          base_fare_usd,
          hotel_name,
          nights,
          rooms,
          check_in_time,
          check_out_time,
          hotel_per_room_usd,
          session_start,
          session_end,
          flight_discount,
          hotel_discount,
          flight_discount_amount,
          hotel_discount_amount,
          flight_booked,
          hotel_booked,
          page_clicks,
          cancellation,
          birthdate,
          gender,
          married,
          has_children,
          home_country,
          home_city,
          home_airport,
          home_airport_lat,
          home_airport_lon,
          sign_up_date
        FROM
          sessions
          LEFT JOIN flights ON flights.trip_id = sessions.trip_id
          LEFT JOIN hotels ON hotels.trip_id = sessions.trip_id
          LEFT JOIN users ON users.user_id = sessions.user_id
        WHERE
          session_start >= '2023-01-04'
          AND sessions.user_id IN (
            SELECT
              user_id
            FROM
              sessions
            WHERE
              session_start >= '2023-01-04'
            GROUP BY
              user_id
            HAVING
              COUNT(session_id) > 7
          )
      )
    
    SELECT
      user_id,
      ROUND(AVG(page_clicks), 2) avg_page_clicks, -- average page clicks for a customer
      ROUND(AVG(EXTRACT(EPOCH FROM (session_end - session_start))), 2) AS avg_session, -- average time spent each session for a customer in seconds
      COALESCE(AVG(ADS), 0) AS ads_km, -- average dollar saved for each actual kilometer travelled by a customer
      COALESCE(AVG(discount_flight_proportion), 0) AS disc_flight_prop, -- average flight ticket bought with discount against all flight ticket purchases for a customer
      COALESCE(ROUND(AVG(flight_discount_amount), 2), 0) AS avg_flight_disc, -- average flight discount received for a customer
      COALESCE(AVG(ADS_hotel), 0) AS ads_night, -- average dollar saved per night for a customer
      COALESCE(AVG(discount_hotel_proportion), 0) AS disc_hotel_prop, -- average hotel booking with discount against all flight ticket purchases for a customer
      COALESCE(ROUND(AVG(hotel_discount_amount), 2), 0) AS avg_hotel_disc, -- average hotel discount received for a customer
    	COALESCE(ROUND(AVG(trip_dur), 2), 0) AS avg_trip_dur, --•	Average Trip Duration measured as the difference between departure time and return time of the user
    	COALESCE(ROUND(AVG(bags),2), 0) AS avg_bags -- average check in luggages for a customer
    FROM
      cohort_filter cd
      LEFT JOIN cohort_ads ads ON cd.trip_id = ads.trip_id
      LEFT JOIN cohort_ads_per_night adspn ON cd.trip_id = adspn.trip_id
    	LEFT JOIN cohort_trip_dur ctd ON cd.trip_id = ctd.trip_id
    	LEFT JOIN cohort_bag cb ON cd.trip_id = cb.trip_id
      LEFT JOIN cohort_disc_flight_prop cdfp ON cd.trip_id = cdfp.trip_id
      LEFT JOIN cohort_disc_hotel_prop cdhp ON cd.trip_id = cdhp.trip_id
    GROUP BY
      user_id
    ORDER BY
      user_id
  )
  
SELECT *
FROM cohort_aggr;