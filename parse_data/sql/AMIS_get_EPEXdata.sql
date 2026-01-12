SELECT 
    IdDatumUurMinuut_UTC,
    Verkoop_prijs_EuroPerMWh
FROM DS_E_Handel_Realisatie
    WHERE Partij_Id = 11
    AND Asset_Id = 18
    AND IdDatumUurMinuut_UTC >= {IdDatumUurMinuutUTC_lowerbound}
    AND IdDatumUurMinuut_UTC < {IdDatumUurMinuutUTC_higherbound}