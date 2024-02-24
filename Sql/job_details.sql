WITH latest_parant
     AS (SELECT job_id,
                CASE
                  WHEN ROW_NUMBER()
                         OVER(
                           ORDER BY start_time DESC) = 1 THEN 'Y'
                  ELSE 'N'
                END AS is_latest_parent
         FROM   lh_bronze.dbo.t_job
         WHERE  job_id IN (SELECT parent_job_id
                           FROM   lh_bronze.dbo.t_job
                           WHERE  parent_job_id IS NOT NULL)
                AND parent_job_id IS NULL),
     base
     AS (SELECT jb.*,
                Datediff(second, jb.start_time, COALESCE(jb.end_time, Getdate())) AS duration_sec,
                jbp.job_name                                                      AS parent_job_name,
                lp.is_latest_parent
         FROM   lh_bronze.dbo.t_job jb
                LEFT OUTER JOIN lh_bronze.dbo.t_job jbp
                             ON ( jbp.job_id = jb.parent_job_id )
                LEFT OUTER JOIN latest_parant lp
                             ON ( lp.job_id = jb.job_id
                                   OR lp.job_id = jb.parent_job_id ))
SELECT job_category,
       job_name,
       parent_job_name,
       is_latest_parent,
       start_time,
       end_time,
       CASE
         WHEN duration_sec < 60 THEN Cast(duration_sec AS VARCHAR) + ' sec'
         WHEN duration_sec < 3600 THEN Cast(duration_sec / 60 AS VARCHAR)
                                       + ' min ' + Cast(duration_sec % 60 AS VARCHAR)
                                       + ' sec'
         WHEN duration_sec < 86400 THEN Cast(duration_sec / 3600 AS VARCHAR)
                                        + ' hrs '
                                        + Cast((duration_sec % 3600) / 60 AS VARCHAR)
                                        + ' min'
         ELSE Cast(duration_sec / 86400 AS VARCHAR)
              + ' day '
              + Cast((duration_sec % 86400) / 3600 AS VARCHAR)
              + ' hrs '
              + Cast(((duration_sec % 86400) % 3600) / 60 AS VARCHAR)
              + ' min'
       END AS time_elapsed,
       status,
       message,
       duration_sec,
       job_id,
       parent_job_id
FROM   base
--WHERE  is_latest_parent = 'Y'
ORDER  BY start_time DESC 