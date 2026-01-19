SELECT 
    *
FROM {import_table}
    WHERE IdDatumUurMinuut_UTC >= {IdDatumUurMinuutUTC_lowerbound}
    AND IdDatumUurMinuut_UTC < {IdDatumUurMinuutUTC_higherbound}