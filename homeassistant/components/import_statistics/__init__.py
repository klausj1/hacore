"""The import_statistics integration."""

import csv
from datetime import datetime
import logging
import os
import zoneinfo

from homeassistant.components.recorder.statistics import async_add_external_statistics
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.typing import ConfigType

DOMAIN = "import_statistics"
_LOGGER = logging.getLogger(__name__)

# Use empty_config_schema because the component does not have any config options
CONFIG_SCHEMA = cv.empty_config_schema(DOMAIN)

ATTR_NAME = "filename"
DEFAULT_NAME = "statisticdata.tsv"


def setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up is called when Home Assistant is loading our component."""

    def handle_import_from_tsv(call):
        """Handle the service call."""
        filename = call.data.get(ATTR_NAME, DEFAULT_NAME)
        _LOGGER.info("Importing statistics from file: " + filename)  # noqa: G003

        hass.states.set("import_statistics.import_from_tsv", filename)
        base_path = "config"
        file_path = f"{base_path}/{filename}"

        if not os.path.exists(file_path):
            _LOGGER.warning(f"filename {filename} does not exist in config folder")  # noqa: G004
            raise HomeAssistantError(
                f"filename {filename} does not exist in config folder"
            )

        with open(file_path, encoding="UTF-8") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter="\t")
            columns = next(csv_reader)
            if not _check_columns(columns):
                _LOGGER.warning(
                    f"filename {filename} does not contain at least one of these columns: statistic_id, start,min, max, mean"  # noqa: G004
                )
                raise HomeAssistantError(
                    f"filename {filename} does not contain at least one of these columns: statistic_id, start,min, max, mean"
                )
            stats = {}
            for row in csv_reader:
                statistic_id = row[_find_index(columns, "statistic_id")]
                if statistic_id not in stats:
                    metadata = {
                        "has_mean": _find_index(columns, "mean") >= 0,
                        "has_sum": _find_index(columns, "sum") >= 0,
                        "source": statistic_id.split(":")[0],
                        "statistic_id": statistic_id,
                        "name": "",
                        "unit_of_measurement": "",
                    }
                    stats[statistic_id] = (metadata, [])

                timezone = zoneinfo.ZoneInfo("Europe/Vienna")

                new_stat = {
                    "start": datetime.strptime(
                        row[_find_index(columns, "start")], "%d.%m.%Y %H:%M"
                    ).replace(tzinfo=timezone),
                    "min": row[_find_index(columns, "min")],
                    "max": row[_find_index(columns, "max")],
                    "mean": row[_find_index(columns, "mean")],
                }
                stats[statistic_id][1].append(new_stat)

        for stat in stats.values():
            metadata = stat[0]
            statistics = stat[1]
            _LOGGER.debug("Calling async_add_external_statistics with:")
            _LOGGER.debug("Metadata:")
            _LOGGER.debug(metadata)
            _LOGGER.debug("Statistics:")
            _LOGGER.debug(statistics)
            async_add_external_statistics(hass, metadata, statistics)

    hass.services.register(DOMAIN, "import_from_tsv", handle_import_from_tsv)

    # Return boolean to indicate that initialization was successful.
    return True


def _find_index(lst, item):
    try:
        index = lst.index(item)
        return index
    except ValueError:
        return -1


def _check_columns(columns):
    # statistic_id	start	min	max	mean
    try:
        columns.index("statistic_id")
        columns.index("start")
        columns.index("min")
        columns.index("max")
        columns.index("mean")
        return True
    except ValueError:
        return False
