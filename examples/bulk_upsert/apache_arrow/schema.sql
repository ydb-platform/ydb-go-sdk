DROP TABLE IF EXISTS `weather`;

CREATE TABLE IF NOT EXISTS `weather` (
    `ID` Uint64 NOT NULL,
    `Date` Text NOT NULL,
    `MaxTemperatureF` Int64,
    `MeanTemperatureF` Int64,
    `MinTemperatureF` Int64,
    `MaxDewPointF` Int64,
    `MeanDewPointF` Int64,
    `MinDewpointF` Int64,
    `MaxHumidity` Int64,
    `MeanHumidity` Int64,
    `MinHumidity` Int64,
    `MaxSeaLevelPressureIn` Double,
    `MeanSeaLevelPressureIn` Double,
    `MinSeaLevelPressureIn` Double,
    `MaxVisibilityMiles` Int64,
    `MeanVisibilityMiles` Int64,
    `MinVisibilityMiles` Int64,
    `MaxWindSpeedMPH` Int64,
    `MeanWindSpeedMPH` Int64,
    `MaxGustSpeedMPH` Int64,
    `PrecipitationIn` Double,
    `CloudCover` Int64,
    `Events` Text,
    `WindDirDegrees` Text,
    `city` Text,
    `season` Text,
    PRIMARY KEY (`ID`)
);

DROP TOPIC `commits`;

CREATE TOPIC `commits`;