#!/bin/bash
for i in {1..10}
do
    echo ""
    echo ""
    echo "($i/10) ------------------------------------------------"
    echo ""
    echo ""
    rm data/mp_sa.sql
    python mp_sa.py
done
