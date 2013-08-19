#!/bin/bash
for i in {1..10}
do
    rm data/mp_sa.sql
    python mp_sa.py
done
