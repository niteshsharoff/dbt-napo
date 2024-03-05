/*
  GIVEN
    our training user level metrics table

  WHEN
    we search for customers who've attended a first class

  THEN
    we expect the number of classes attended to be > 0
*/
select *
from {{ ref("training_user_level_metrics") }}
where first_class_attended is not null and classes_attended = 0
