#!/bin/sh
mysql --skip-column-names --batch -u root cinder -e 'select id,unix_timestamp(deleted_at) from volumes where deleted = 1;'