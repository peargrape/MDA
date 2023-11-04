-- Session building on SQL
-- This is an advanced version of gaps-and-islands problem. StackOverflow was very helpful, see https://stackoverflow.com/questions/62904977/how-to-get-non-overlapping-distinct-intervals-in-postgresql-table 

WITH events_offline_dt AS (
SELECT
  e.user_id,
  CASE
    WHEN e.event_type = 'offline' THEN TRUE
    ELSE FALSE
  END AS offline,
  e.dt AS dt_start,
  CASE
    WHEN e.event_type = 'action' THEN e.dt
    WHEN e.event_type = 'meeting' THEN e.dt + (e.event_props * interval '1 second')
    ELSE NULL
  END AS dt_end,
  CASE
    WHEN e.event_type IN ('action', 'meeting') THEN e.dt + (COALESCE(e.event_props, 0) * interval '1 second') + interval '15 minutes'
    ELSE NULL
  END AS dt_end_15_mins,
  CASE WHEN e.event_type IN ('action', 'meeting') THEN 
  (SELECT MIN(dt)
   FROM events
   WHERE user_id = e.user_id
     AND event_type = 'offline'
     AND dt > e.dt) 
  ELSE NULL END AS dt_end_offline
FROM events e
WHERE event_type <> 'offline'),
s AS (
SELECT 
   events_offline_dt.*, 
   lag(dt_end_15_mins) OVER (PARTITION BY user_id ORDER BY dt_start) lag_dt_15_mins
FROM events_offline_dt), 
s1 AS (
SELECT 
   s.*,
   count(*) FILTER(WHERE dt_start > lag_dt_15_mins) OVER (PARTITION BY user_id, dt_end_offline ORDER BY dt_start) grp
FROM s)
SELECT 
  user_id, 
  min(dt_start) dt_start,
  CASE WHEN min(dt_start) < now() AND (max(LEAST(COALESCE(dt_end_offline, dt_end_15_mins), dt_end_15_mins)) >= now()) 
  THEN NULL 
  ELSE max(LEAST(COALESCE(dt_end_offline, dt_end), dt_end)) 
  END AS dt_end, 
  dt_end_offline
FROM s1
GROUP BY user_id , grp, dt_end_offline
ORDER BY 1, 2;