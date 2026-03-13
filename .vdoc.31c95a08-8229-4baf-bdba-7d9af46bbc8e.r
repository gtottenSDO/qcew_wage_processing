#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
library(tidyverse)
library(scales)
library(DBI)
library(RPostgres)
library(sdodemog)
library(dbplyr)
#
#
#
#
#
# connect to postgres database
# Create Connection to Postgres Database
con <- connect_to_sdo_db("gtotten", password =Sys.getenv("sdo_db_pw"))

#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
geography_crosswalk <- tbl(con, Id(schema = "xwalk", table = "area_pm"))


jobs_by_sector <- tbl(
  con,
  Id(schema = "estimates", table = "jobs_by_sector")
) %>%
  mutate(area_code = str_pad(area_code, 3, pad = "0")) %>%
  select(
    fips_long = geonum,
    county_fips = area_code,
    naics = sector_id,
    year = population_year,
    total = total_jobs
  ) %>%
  filter(total != 0)

# leifa <- leifa_long %>%
#   filter(leifa_group == "Jobs")

#
#
#
#
#
#
#
cty_list <- geography_crosswalk |>
  distinct(county_fips) |>
  pull() |>
  sort()

year_list <- c(2001:2024)

# create template for county by year
county_year_template <- expand_grid(county_fips = cty_list, year = year_list)

naics_sectors <- pull_db_table("xwalk", "naics_long") |>
  filter(lvl %in% c(0, 2)) |>
  pull(naics)

# create template for county by gcode and year
county_naics_year_template <- expand_grid(
  county_fips = cty_list,
  naics = naics_sectors,
  year = year_list
)

#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
# load from postgres

sdo_emp <- pull_db_table("econ", "jobs_estimates", filter_model_id = TRUE)

qcew_data <- tbl(con, Id(schema = "econ", table = "qcew_lmi")) |>
  filter(
    (ind_code %in% c("00", naics_sectors)) &
      ((own_code %in% c("00", "10", "20", "30") & ind_code == "00") |
        (own_code == "50" & ind_code %in% naics_sectors))
  ) |>
  # reclassify ownership codes for public sectors to be "1", "2", "3" for federal, state, local respectively
  mutate(
    ind_code = if_else(
      own_code %in% c("10", "20", "30"),
      str_sub(own_code, 1, 1),
      ind_code
    ),
    ind_title = if_else(
      own_code %in% c("10", "20", "30"),
      own_title,
      ind_title
    )
  ) |>
  left_join(
    geography_crosswalk,
    by = join_by(county_fips)
  ) |>
  pivot_wider(names_from = area_type, values_from = area_id) |>
  # replace "s;" flags in avg_emp:tax_wage with NA and convert to numeric
  mutate(
    across(avg_emp:tax_wage, ~ as.numeric(na_if(., "s;")))
  ) |>
  # add county and region total employment total wages and avg wages
  mutate(
    region_avg_emp = na_if(sum(avg_emp, na.rm = TRUE), 0),
    region_tot_wage = na_if(sum(tot_wage, na.rm = TRUE), 0),
    .by = c(year, ind_code, region)
  ) |>
  collect() |>
  mutate(
    state_avg_emp = avg_emp[county_fips == "000"],
    state_tot_wage = tot_wage[county_fips == "000"],
    .by = c(year, ind_code)
  ) |>
  mutate(
    avg_emp_final = coalesce(avg_emp, region_avg_emp, state_avg_emp),
    tot_wage_final = coalesce(tot_wage, region_tot_wage, state_tot_wage),
    avg_wage = tot_wage_final / na_if(avg_emp_final, 0)
  ) |>
  left_join(
    sdo_emp |>
      select(
        year,
        county_fips = area_id,
        ind_code,
        sdo_jobs = jobs
      ) |>
      mutate(
        ind_code = if_else(
          ind_code == "10",
          "00",
          ind_code
        )
      )
  ) |>
  mutate(
    area_code = county_fips,
    population_year = year,
    total_wage = avg_wage[ind_code == "00"],
    category = factor(
      case_when(
        avg_wage < 0.8 * total_wage ~ "Low",
        avg_wage > 1.2 * total_wage ~ "High",
        avg_wage == total_wage ~ "Average",
        TRUE ~ "Mid"
      ),
      levels = c("Low", "Mid", "High", "Average"),
      ordered = TRUE
    ),
    sum_jobs = sdo_jobs[ind_code == "00"],
    pct_jobs = sdo_jobs / na_if(sum_jobs, 0),
    leg_color = case_when(
      category == "Low" ~ "#85BFFF",
      category == "Mid" ~ "#0073F2",
      category == "High" ~ "#003E82",
      .default = NULL
    ),
    dollar_low = scales::dollar(total_wage * .8, accuracy = 1),
    dollar_high = scales::dollar(total_wage * 1.2, accuracy = 1),
    label = case_when(
      category == "Low" ~
        paste0("Less Than ", dollar_low),
      category == "Mid" ~
        paste0(dollar_low, " to ", dollar_high),
      category == "High" ~
        paste0("More Than ", dollar_high),
      .default = NULL
    ),
    .by = c(county_fips, year)
  )

# Upload to postgres
sdo_db_write_table(
  qcew_data,
  schema = "econ",
  table = "wage_category_data",
  vintage = "v2024",
  version = "v1",
  overwrite = TRUE,
  latest = TRUE
)


#
#
#
#
#
wage_boundaries <- qcew_data |>
  filter(ind_code != "00") |>
  summarize(
    cat_jobs = sum(sdo_jobs, na.rm = TRUE),
    min_wage = min(avg_wage, na.rm = TRUE),
    max_wage = max(avg_wage, na.rm = TRUE),
    .by = c(
      area_code,
      population_year,
      category,
      leg_color,
      label,
      sum_jobs,
      total_wage
    )
  ) |>
  mutate(
    pct_jobs = cat_jobs / sum_jobs,
    label = paste0("(", label, ") ", scales::percent(pct_jobs, accuracy = .1)),
    label2 = paste0(
      category,
      " Wage Jobs: ",
      label
    )
  ) |>
  arrange(population_year, area_code, category)


#
#
#
#
#
#
#
jobs_by_sector <- qcew_data %>%
  left_join(
    wage_table %>%
      select(
        county_fips,
        year,
        gcode,
        category,
        avg_annual_wages,
        wage_source
      ) %>%
      rename(category = category),
    by = join_by("county_fips", "year", "gcode")
  ) %>%
  mutate(
    wage_source = ifelse(county_fips == "000", "state", wage_source),
    wage_source_override = ifelse(
      wage_source == "county" | county_fips == "000",
      FALSE,
      TRUE
    )
  ) %>%
  arrange(county_fips, year, gcode) %>%
  select(
    area_code = county_fips,
    geonum = fips_long,
    sector_id = gcode,
    population_year = year,
    total_jobs = total,
    Avg_wage = avg_annual_wages,
    category,
    wage_source,
    wage_source_override
  )

write_csv(jobs_by_sector, "_output/jobs_by_sector_cat_2023.csv")
#
#
#
#
#
#
#
#
#
#
#
#
#
#
average_wage <- wage_table %>%
  select(
    area_code = county_fips,
    population_year = year,
    sector_id = gcode,
    avg_wage = avg_annual_wages,
    category = category,
    wage_source
  ) %>%
  mutate(
    wage_source = ifelse(area_code == "000", "state", wage_source),
    wage_source_override = ifelse(
      wage_source == "county" | area_code == "000",
      FALSE,
      TRUE
    )
  ) %>%
  arrange(area_code, population_year, sector_id)

write_csv(average_wage, "_output/average_wage_2023.csv")
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#

write_csv(
  wage_boundaries %>%
    filter(!(is.na(pct_jobs))),
  "_output/wage_boundaries_2023.csv"
)
#
#
#
#
#
#
#
#
#
#
#
#
#
wage_group_check <- wage_table %>%
  #exclude state and "500" (latter effectively a county)
  filter(wage_source != "county" & !(county_fips %in% c("000", "500"))) %>%
  inner_join(
    wage_table %>%
      filter(wage_source == "county") %>%
      group_by(year, gcode) %>%
      count(category) %>%
      pivot_wider(
        names_from = category,
        values_from = n,
        values_fill = 0
      ) %>%
      mutate(
        total = Low + Mid + High,
        Low_pct = Low / total,
        Mid_pct = Mid / total,
        High_pct = High / total,
        most_frequent = factor(
          case_when(
            Low_pct > Mid_pct & Low_pct > High_pct ~ "Low",
            Mid_pct > Low_pct & Mid_pct > High_pct ~ "Mid",
            High_pct > Low_pct & High_pct > Mid_pct ~ "High",
            TRUE ~ "Tie"
          ),
          levels = c("Low", "Mid", "High", "Average"),
          ordered = TRUE
        )
      ) %>%
      ungroup() %>%
      select(-total),
    by = join_by("year", "gcode")
  ) %>%
  filter(
    category != most_frequent & year == 2023
  ) %>%
  left_join(
    leifa_public %>%
      filter(year == 2023) %>%
      select(county_fips, gcode, total)
  ) %>%
  filter(total != 0) %>%
  left_join(
    regional_averages %>%
      filter(year == 2023) %>%
      select(region_type, region_id, gcode, total_annual_employment)
  ) %>%
  filter(total_annual_employment < 2000) %>%
  mutate(
    designation_diff <- abs(
      as.numeric(category) -
        as.numeric(most_frequent)
    )
  )


print(wage_group_check %>% group_by(county_fips) %>% count(sort = TRUE))
print(wage_group_check %>% group_by(gcode) %>% count(sort = TRUE)) %>%
  left_join(select(gcode_n2_crosswalk, gcode:gcode_label))
#
#
#
#
#
#
#
suffixes <- c("_2023", "_2022")

wage_boundaries_v22 <- vroom("_data/wage_boundaries_2022.csv")

wage_boundaries_comparison <- wage_boundaries %>%
  left_join(
    wage_boundaries_v22,
    by = c(
      "area_code" = "area_code",
      "population_year" = "population_year",
      "category" = "category"
    ),
    suffix = suffixes
  ) %>%
  mutate(
    cat_jobs_diff = cat_jobs_2023 - cat_jobs_2022,
    cat_jobs_pct_diff = cat_jobs_diff / cat_jobs_2022,
    sum_jobs_diff = round(sum_jobs_2023 - sum_jobs_2022, 0),
    sum_jobs_pct_diff = sum_jobs_diff / sum_jobs_2022,
    pct_jobs_diff = pct_jobs_2023 - pct_jobs_2022,
    min_wage_diff = min_wage_2023 - min_wage_2022,
    max_wage_diff = max_wage_2023 - max_wage_2022,
    total_wage_diff = total_wage_2023 - total_wage_2022
  )

write_csv(wage_boundaries_comparison, "_data/wage_boundaries_comparison.csv")


#
#
#
#
#
jobs_by_sector_v22 <- vroom("_data/jobs_by_sector_cat_2022.csv") %>%
  mutate(
    category = factor(
      category,
      levels = c("Low", "Mid", "High"),
      ordered = TRUE
    )
  )

jobs_by_sector_missing <- jobs_by_sector %>%
  anti_join(
    jobs_by_sector_v22,
    by = c(
      "area_code" = "area_code",
      "population_year" = "population_year",
      "sector_id" = "sector_id"
    )
  ) %>%
  filter(population_year != 2023 & sector_id != "000000")

jobs_by_sector_comparison <- jobs_by_sector %>%
  left_join(
    jobs_by_sector_v22,
    by = c(
      "area_code" = "area_code",
      "population_year" = "population_year",
      "sector_id" = "sector_id"
    ),
    suffix = suffixes
  ) %>%
  mutate(
    jobs_diff = total_jobs_2023 - total_jobs_2022,
    jobs_pct_diff = jobs_diff / total_jobs_2022,
    Avg_wage_diff = Avg_wage_2023 - Avg_wage_2022,
    Avg_wage_pct_diff = Avg_wage_diff / Avg_wage_2022,
    category_diff = category_2023 - category_2022
  ) %>%
  left_join(select(
    gcode_n2_crosswalk,
    sector_id = gcode,
    sector_label = gcode_label
  )) %>%
  relocate(sector_label, .after = sector_id)

jobs_by_sector_missing_wage <- jobs_by_sector %>%
  filter(is.na(Avg_wage) & sector_id %in% gcode_n2_crosswalk$gcode)

write_csv(jobs_by_sector_comparison, "_data/jobs_by_sector_comparison.csv")
#
#
#
#
#
#
