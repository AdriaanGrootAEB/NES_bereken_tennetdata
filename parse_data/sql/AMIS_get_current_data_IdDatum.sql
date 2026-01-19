SELECT 
    *
FROM {import_table}
    WHERE IdDatum>= {IdDatum_lowerbound}
    AND IdDatum < {IdDatum_higherbound}