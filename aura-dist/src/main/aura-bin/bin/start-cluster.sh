#!/bin/bash
########################################################################################################################
# 
#  Copyright (C) 2010-2013 by the Aura project (http://aura.eu)
# 
#  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
#  the License. You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
#  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
#  specific language governing permissions and limitations under the License.
# 
########################################################################################################################

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/env.sh

HOSTLIST=$AURA_SLAVES

if [ "$HOSTLIST" = "" ]; then
    HOSTLIST="${AURA_CONF_DIR}/slaves"
fi

if [ ! -f $HOSTLIST ]; then
    echo $HOSTLIST is not a valid slave list
    exit 1
fi


# cluster mode, bring up workload manager locally and a task manager on every slave host
$AURA_BIN_DIR/wm.sh start cluster

GOON=true
while $GOON
do
    read HOST || GOON=false
    if [ -n "$HOST" ]; then
        ssh -n $AURA_SSH_OPTS $HOST -- "nohup /bin/bash $AURA_BIN_DIR/tm.sh start &"
    fi
done < $HOSTLIST
