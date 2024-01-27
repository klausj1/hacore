"""The import_statistics integration."""

import csv
from datetime import datetime
import logging
import zoneinfo

from homeassistant.components.recorder.statistics import async_add_external_statistics
from homeassistant.core import HomeAssistant
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.typing import ConfigType

DOMAIN = "import_statistics"

# Use empty_config_schema because the component does not have any config options
CONFIG_SCHEMA = cv.empty_config_schema(DOMAIN)

ATTR_NAME = "filename"
DEFAULT_NAME = "mydata.tsv"

# CONFIG_SCHEMA = "cv.empty_config_schema"


def setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up is called when Home Assistant is loading our component."""

    def handle_import_from_tsv(call):
        """Handle the service call."""
        filename = call.data.get(ATTR_NAME, DEFAULT_NAME)
        logging.info("Importing statistics from file: " + filename)  # noqa: G003

        hass.states.set("import_statistics.import_from_tsv", filename)
        base_path = "config"
        file_path = f"{base_path}/{filename}"

        with open(file_path, encoding="UTF-8") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter="\t")
            stats = {}
            for idx, row in enumerate(csv_reader):
                if idx == 0:
                    # get cols
                    columns = row
                    logging.debug(columns)
                else:
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
            logging.info("Metadata:")
            logging.info(metadata)
            logging.info("Statistics:")
            logging.info(statistics)
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
