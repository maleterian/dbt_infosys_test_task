-- depends on {{ ref('distinctCountryCount') }}
SELECT
  a.reportDate,
  a.operatingSystem,
  a.distinctCountryCount,
  b.totalPageViews
FROM  
   {{ ref('distinctCountryCount') }} a
JOIN
  {{ ref('totalPageViews') }} b
ON ( a.reportDate = b.reportDate and 
     a.operatingSystem = b.operatingSystem )
