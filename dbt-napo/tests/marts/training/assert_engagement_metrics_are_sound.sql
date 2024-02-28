/*
  GIVEN
    our training user level metrics table

  WHEN
    we sanity check our engagement metrics

  THEN
    we expect:
      classes scheduled >= classes joined
      classes attended >= classes joined
      classes scheduled >= classes cancelled
      classes scheduled >= classes missed
      sesssions scheduled >= sesssions joined
      sesssions attended >= sesssions joined
      sesssions scheduled >= sesssions cancelled
      sesssions scheduled >= sesssions missed
*/
select *
from {{ ref("training_user_level_metrics") }}
where
    classes_joined > classes_scheduled
    or classes_attended > classes_joined
    or classes_cancelled > classes_scheduled
    or classes_missed > classes_scheduled
    or sessions_joined > sessions_scheduled
    or sessions_attended > sessions_joined
    or sessions_cancelled > sessions_scheduled
    or sessions_missed > sessions_scheduled
