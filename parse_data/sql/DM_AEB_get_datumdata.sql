SELECT 
    IdDatumUurMinuut_UTC,
    IdDatumUurMinuut,
    IdDatum,
    DatumUurMinuut
FROM VW_AEB_D_DatumUurMinuut
    WHERE IdDatumUurMinuut >= {IdDatumUurMinuut_lowerbound}
    AND IdDatumUurMinuut < {IdDatumUurMinuut_higherbound}