SELECT 
    IdDatumUurMinuut_UTC,
    IdDatumUurMinuut,
    IdDatum,
    DatumUurMinuut
FROM VW_AEB_D_DatumUurMinuut
    WHERE IdDatumUurMinuut_UTC >= {IdDatumUurMinuutUTC_lowerbound}
    AND IdDatumUurMinuut_UTC < {IdDatumUurMinuutUTC_higherbound}