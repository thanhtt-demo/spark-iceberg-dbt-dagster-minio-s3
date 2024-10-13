from dagster import (
    DailyPartitionsDefinition,
    # MonthlyPartitionsDefinition,
    # WeeklyPartitionsDefinition,
)

from ..assets import constants

start_date = constants.START_DATE
# end_date = constants.END_DATE

# monthly_partition = MonthlyPartitionsDefinition()

# weekly_partition = WeeklyPartitionsDefinition()

daily_partition = DailyPartitionsDefinition(start_date=start_date, timezone='Asia/Ho_Chi_Minh')
