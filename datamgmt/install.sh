#!/usr/bin/env bash
echo "**** Preparing to install the INTCare Proof of Concept."
echo "Please ensure that you updated both config.properties and user-env.sh!"

echo "**** Calling setup_*.sh ..."
if [ `hostname` = master.datamgmt.intcare ]; then
    echo "**** Calling setup_master.sh ..."
    setup/bin/setup_master.sh
else
    echo "**** Calling setup_ingest.sh ..."
    setup/bin/setup_ingest.sh
fi
