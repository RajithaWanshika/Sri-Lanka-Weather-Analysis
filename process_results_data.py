#!/usr/bin/env python3

import json
import os
import re
import csv
from collections import defaultdict


def read_precipitation_data():
    precip_data = defaultdict(lambda: {"year_month": {}, "total": 0})
    file_path = "results/analysis2_highest_precipitation/part-r-00000"
    if not os.path.exists(file_path):
        print(f"Warning: {file_path} not found")
        return {}
    month_names = [
        "",
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]
    with open(file_path, "r") as f:
        for line in f:
            if "|" in line and "HIGHEST" not in line:
                parts = line.strip().split("\t")
                if len(parts) >= 2:
                    try:
                        key_part = parts[0]
                        if "|" in key_part:
                            year, month = key_part.split("|")
                            year = int(year)
                            month = int(month)
                            json_str = parts[1]
                            json_str = re.sub(r"\s+hours", "", json_str)
                            data = json.loads(json_str)
                            precip = float(data.get("totalPrecipitation", 0))
                            month_name = month_names[month]
                            key = f"{year}-{month:02d}"
                            precip_data[key] = {
                                "year": year,
                                "month": month,
                                "month_name": month_name,
                                "precipitation": round(precip, 2),
                            }
                    except Exception as e:
                        pass
    return precip_data


def read_district_monthly_data():
    district_data = defaultdict(
        lambda: {"months": {}, "total": 0, "temp_months": [], "monthly_data": []}
    )
    seasons = {
        1: "Jan-Mar",
        2: "Jan-Mar",
        3: "Jan-Mar",
        4: "Apr-Jun",
        5: "Apr-Jun",
        6: "Apr-Jun",
        7: "Jul-Sep",
        8: "Jul-Sep",
        9: "Jul-Sep",
        10: "Oct-Dec",
        11: "Oct-Dec",
        12: "Oct-Dec",
    }
    month_names = [
        "",
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]
    file_path = "results/analysis1_district_monthly/part-r-00000"
    if not os.path.exists(file_path):
        print(f"Warning: {file_path} not found")
        return {}, {}, {}, {}
    with open(file_path, "r") as f:
        for line in f:
            if "|" in line:
                parts = line.strip().split("\t")
                if len(parts) >= 2:
                    try:
                        data = json.loads(parts[1])
                        district = data["district"]
                        month = data["month"]
                        precip = data["totalPrecipitation"]
                        temp = data["meanTemperature"]
                        district_data[district]["months"][month] = {
                            "precip": precip,
                            "temp": temp,
                            "month_name": month_names[month],
                        }
                        district_data[district]["total"] += precip
                        district_data[district]["temp_months"].append(
                            {"month": month, "temp": temp}
                        )
                        district_data[district]["monthly_data"].append(
                            {
                                "month": month,
                                "month_name": month_names[month],
                                "precipitation": round(precip, 2),
                                "temperature": round(temp, 2),
                            }
                        )
                    except Exception as e:
                        pass

    most_precipitous = {}
    for district, data in district_data.items():
        if data["months"]:
            max_month = max(data["months"].items(), key=lambda x: x[1]["precip"])
            month_num = max_month[0]
            month_info = max_month[1]
            season = seasons[month_num]
            most_precipitous[district] = {
                "month": month_info["month_name"],
                "month_num": month_num,
                "season": season,
                "precipitation": round(month_info["precip"], 2),
            }
    top_5 = sorted(district_data.items(), key=lambda x: x[1]["total"], reverse=True)[:5]
    top_5_list = [
        {"district": d, "total": round(data["total"], 2)} for d, data in top_5
    ]

    temp_analysis = {}
    monthly_precip_temp = {}
    for district, data in district_data.items():
        monthly_precip_temp[district] = sorted(
            data["monthly_data"], key=lambda x: x["month"]
        )
    return most_precipitous, top_5_list, temp_analysis, monthly_precip_temp


def read_weekly_max_temperature_data():
    """Read weekly max temperature data from analysis4 and calculate district-level statistics."""
    temp_analysis = {}
    dir_path = "results/analysis4_weekly_max_temp/2_weekly_temps_by_district"
    if not os.path.exists(dir_path):
        print(f"Warning: {dir_path} not found")
        return {}

    # Find the CSV file in the directory
    csv_files = [f for f in os.listdir(dir_path) if f.endswith(".csv")]
    if not csv_files:
        print(f"Warning: No CSV files found in {dir_path}")
        return {}

    file_path = os.path.join(dir_path, csv_files[0])
    district_temps = defaultdict(list)

    with open(file_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                district = row["district"]
                weekly_max_temp = float(row["weekly_max_temperature_C"])
                district_temps[district].append(weekly_max_temp)
            except (ValueError, KeyError) as e:
                continue

    for district, temps in district_temps.items():
        if temps:
            total_weeks = len(temps)
            weeks_above_30 = sum(1 for t in temps if t > 30)
            weeks_below_30 = total_weeks - weeks_above_30

            avg_max_temp = sum(temps) / len(temps) if temps else 0
            min_temp = min(temps) if temps else 0
            max_temp = max(temps) if temps else 0

            temp_analysis[district] = {
                "weeks_above_30": weeks_above_30,
                "weeks_below_30": weeks_below_30,
                "percentage_above_30": round(
                    (weeks_above_30 / total_weeks * 100) if total_weeks > 0 else 0, 1
                ),
                "percentage_below_30": round(
                    (weeks_below_30 / total_weeks * 100) if total_weeks > 0 else 0, 1
                ),
                "avg_max_temperature_C": round(avg_max_temp, 2),
                "min_temperature": round(min_temp, 2),
                "max_temperature": round(max_temp, 2),
                "total_weeks": total_weeks,
            }

    return temp_analysis


def calculate_extreme_events():
    district_data = defaultdict(lambda: {"precip_values": []})
    year_data = defaultdict(lambda: {"precip_values": []})
    file_path = "results/analysis1_district_monthly/part-r-00000"
    if not os.path.exists(file_path):
        print(f"Warning: {file_path} not found")
        return {}, {}
    all_precip = []
    with open(file_path, "r") as f:
        for line in f:
            if "|" in line:
                parts = line.strip().split("\t")
                if len(parts) >= 2:
                    try:
                        data = json.loads(parts[1])
                        district = data["district"]
                        precip = data["totalPrecipitation"]
                        district_data[district]["precip_values"].append(precip)
                        all_precip.append(precip)
                    except:
                        pass
    precip_file = "results/analysis2_highest_precipitation/part-r-00000"
    if os.path.exists(precip_file):
        with open(precip_file, "r") as f:
            for line in f:
                if "|" in line and "HIGHEST" not in line:
                    parts = line.strip().split("\t")
                    if len(parts) >= 2:
                        try:
                            key_part = parts[0]
                            if "|" in key_part:
                                year, month = key_part.split("|")
                                year = int(year)
                                json_str = parts[1]
                                json_str = re.sub(r"\s+hours", "", json_str)
                                data = json.loads(json_str)
                                precip = float(data.get("totalPrecipitation", 0))
                                year_data[year]["precip_values"].append(precip)
                        except:
                            pass
    sorted_precip = sorted(all_precip)
    precip_threshold = (
        sorted_precip[int(len(sorted_precip) * 0.75)] if sorted_precip else 0
    )
    extreme_events = {}
    for district, data in district_data.items():
        if data["precip_values"]:
            count = sum(
                1 for precip in data["precip_values"] if precip > precip_threshold
            )
            extreme_events[district] = count
    extreme_events_by_year = {}
    for year, data in year_data.items():
        if data["precip_values"]:
            count = sum(
                1 for precip in data["precip_values"] if precip > precip_threshold
            )
            extreme_events_by_year[year] = count
    return extreme_events, extreme_events_by_year


def main():
    print("Processing results data...")
    most_precipitous, top_5_districts, _, monthly_precip_temp = (
        read_district_monthly_data()
    )
    # Use weekly max temperature data for temperature analysis
    temp_analysis = read_weekly_max_temperature_data()
    extreme_events, extreme_events_by_year = calculate_extreme_events()
    output = {
        "most_precipitous": most_precipitous,
        "top_5_districts": top_5_districts,
        "temp_analysis": temp_analysis,
        "monthly_precip_temp": monthly_precip_temp,
        "extreme_events": extreme_events,
        "extreme_events_by_year": extreme_events_by_year,
    }
    output_path = "results/dashboard_data.json"
    os.makedirs("results", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n✓ Data processed and saved to {output_path}")
    print(f"  - Most precipitous: {len(most_precipitous)} districts")
    print(f"  - Top 5 districts: {len(top_5_districts)} districts")
    print(f"  - Temperature analysis: {len(temp_analysis)} districts")
    print(f"  - Monthly data: {len(monthly_precip_temp)} districts")
    print(f"  - Extreme events: {len(extreme_events)} districts")
    print(f"  - Extreme events by year: {len(extreme_events_by_year)} years")


if __name__ == "__main__":
    main()
