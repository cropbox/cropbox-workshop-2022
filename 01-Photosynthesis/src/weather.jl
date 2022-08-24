using Cropbox

using CSV
using DataFrames
using Dates
using TimeZones

include("vaporpressure.jl")

@system Weather begin
    vp(context): vapor_pressure ~ ::VaporPressure
    VPD(T_air, RH, D=vp.D): vapor_pressure_deficit => D(T_air, RH) ~ track(u"kPa")
    VPD_Δ(T_air, Δ=vp.Δ): vapor_pressure_saturation_slope_delta => Δ(T_air) ~ track(u"kPa/K")
    VPD_s(T_air, P_air, s=vp.s): vapor_pressure_saturation_slope => s(T_air, P_air) ~ track(u"K^-1")

    k: radiation_conversion_factor => (1 / 4.55) ~ preserve(u"J/μmol")
    PFD: photon_flux_density ~ preserve(parameter, u"μmol/m^2/s")
    solrad(PFD, k): solar_radiation => (PFD * k) ~ track(u"W/m^2")

    T_air: air_temperature ~ preserve(parameter, u"°C")
    Tk_air(T_air): absolute_air_temperature ~ track(u"K")

    rain: precipitation ~ preserve(parameter, u"mm")
    wind: wind_speed ~ preserve(parameter, u"m/s")
    RH: relative_humidity ~ preserve(parameter, u"percent")

    CO2: carbon_dioxide ~ preserve(parameter, u"μmol/mol")
    P_air: air_pressure ~ preserve(parameter, u"kPa")

    lat: latitude ~ preserve(parameter, u"°")
end

@system WeatherData(Weather) begin
    calendar(context) ~ ::Calendar(override)
    data ~ provide(index = :index, init = calendar.time, parameter)

    PFD: photon_flux_density ~ drive(from = data, by = :Irrad, u"μmol/m^2/s") #Quanta
    T_air: air_temperature ~ drive(from = data, by = :Tair, u"°C")

    rain: precipitation ~ drive(from = data, by = :rain, u"mm")
    wind: wind_speed ~ drive(from = data, by = :wind, u"m/s")
    RH: relative_humidity ~ drive(from = data, by = :RH, u"percent")
end

#HACK: test alternative implementation using array as parameter
@system WeatherData2(Weather) begin
    PFD: photon_flux_density ~ drive(parameter, u"μmol/m^2/s") #Quanta
    T_air: air_temperature ~ drive(parameter, u"°C")

    rain: precipitation ~ drive(parameter, u"mm")
    wind: wind_speed ~ drive(parameter, u"m/s")
    RH: relative_humidity ~ drive(parameter, u"percent")
end

#HACK: handle different API for Fixed/VariableTimeZone
zoned_datetime(dt::DateTime, tz::TimeZone, occurrence=1) = ZonedDateTime(dt, tz)
zoned_datetime(dt::DateTime, tz::VariableTimeZone, occurrence=1) = ZonedDateTime(dt, tz, occurrence)

loadcsv(filename; timezone = tz"Asia/Seoul", indexkey = :index) = begin
    df = CSV.File(filename) |> DataFrame
    df[!, indexkey] = map(r -> begin
        occurrence = 1
        i = DataFrames.row(r)
        if i > 1
            r0 = parent(r)[i-1, :]
            r0.timestamp == r.timestamp && (occurrence = 2)
        end
        dt = Dates.DateTime(r.timestamp, "yyyy-mm-dd HH:MM:SS")
        zoned_datetime(dt, timezone, occurrence)
    end, eachrow(df))
    df
end
