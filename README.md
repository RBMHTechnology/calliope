# Calliope

## Current status

This project contains components used for system integrations over a federated event-bus. Work is done on [`wip-eventbus-` branches](https://github.com/RBMHTechnology/calliope/branches/all?utf8=%E2%9C%93&query=wip-eventbus-) that serve as basis for later work on `wip-eventbus`. 

## Documentation

Coming soon ...


sequence_nrs
------------------------
| topic       PK       |
| partition   PK       | 
| source_id   PK       |
| sequence_nr          |
------------------------

SQL:

UPDATE  sequence_nrs
  SET   sequence_nr = p_sequence_nr
  WHERE topic =       p_topic
  AND   partition =   p_partition
  AND   source_id =   p_source_id
  AND   sequence_nr < p_sequence_nr