# Install once if needed:
# install.packages(c("DBI", "RSQLite", "dplyr", "ggplot2", "rdrobust", "rddensity"))

library(DBI)
library(RSQLite)
library(dplyr)
library(ggplot2)
library(rdrobust)
library(rddensity)

db_path  <- "C:/Stanford/2026F/STAT_209/project/uni_vehicles.db"
table_nm <- "unified_vehicle_listings"
con <- dbConnect(RSQLite::SQLite(), db_path)

car_tbl <- tbl(con, table_nm)

run_rdd_for_vehicle <- function(make0  = NULL,
                                model0 = NULL,
                                year0  = NULL,
                                trim0  = NULL,
                                cutoff_miles = 100000,
                                max_window   = 20000,
                                db_path      = db_path,
                                table_nm     = table_nm) {
  cat("\n====================================\n")
  cat("   RDD PIPELINE START\n")
  cat("====================================\n\n")
  
  cat("---- 0. PARAMETERS ----\n")
  cat("make:  ", ifelse(is.null(make0),  "ALL", make0),  "\n")
  cat("model: ", ifelse(is.null(model0), "ALL", model0), "\n")
  cat("year:  ", ifelse(is.null(year0),  "ALL", year0),  "\n")
  cat("trim:  ", ifelse(is.null(trim0),  "ALL", trim0),  "\n")
  cat("cutoff (miles): ", cutoff_miles, "\n")
  cat("max window (+/- miles): ", max_window, "\n\n")
  
  ## 1. CONNECT & LOAD DATA ----
  cat("==== 1. CONNECTING TO DB & LOADING DATA ====\n")
  
  # Conditional filtering depending on which arguments are NULL
  if (!is.null(make0))  car_tbl <- car_tbl %>% filter(make  == make0)
  if (!is.null(model0)) car_tbl <- car_tbl %>% filter(model == model0)
  if (!is.null(year0))  car_tbl <- car_tbl %>% filter(year  == year0)
  if (!is.null(trim0))  car_tbl <- car_tbl %>% filter(trim  == trim0)
  
  car_df <- car_tbl %>%
    select(make, model, year, trim, price, mileage) %>%
    collect()
  
  cat("Number of raw rows pulled: ", nrow(car_df), "\n\n")
  
  if (nrow(car_df) == 0) {
    cat(">>> No observations for this selection. Exiting.\n")
    return(invisible(NULL))
  }
  
  ## 2. CLEANING & RUNNING VARIABLE ----
  cat("==== 2. CLEANING DATA & DEFINING RUNNING VARIABLE ====\n")
  
  car_df <- car_df %>%
    mutate(
      price   = as.numeric(price),
      mileage = as.numeric(mileage)
    ) %>%
    filter(!is.na(price), !is.na(mileage))
  
  # Optional outlier removal
  car_df <- car_df %>%
    filter(
      price   > 1000,   price   < 150000,
      mileage > 10000,  mileage < 300000
    )
  
  car_df <- car_df %>%
    mutate(
      mileage_centered = mileage - cutoff_miles,
      log_price        = log(price),
      above_cutoff     = if_else(mileage >= cutoff_miles, 1L, 0L)
    ) %>%
    filter(abs(mileage_centered) <= max_window)
  
  car_df <- car_df %>%
    filter(
      !is.na(make),
      !is.na(model),
      !is.na(year),
      !is.na(trim)
    )
  
  cat("Rows after cleaning & windowing: ", nrow(car_df), "\n")
  cat("Summary of mileage_centered:\n")
  print(summary(car_df$mileage_centered))
  cat("Counts above vs below cutoff:\n")
  print(table(car_df$above_cutoff))
  cat("\n")
  
  if (nrow(car_df) < 30) {
    cat(">>> WARNING: very few observations after filtering; RDD may be underpowered.\n\n")
  }
  
  ## 3. BASELINE COVARIATES / POOLED LOGIC ----
  cat("==== 3. BUILDING BASELINE COVARIATES (POOLED LOGIC) ====\n")
  
  fe_terms <- c()
  # Only include a variable in the FE/covariates if we did NOT fix it in the filter
  if (is.null(make0))  fe_terms <- c(fe_terms, "make")
  if (is.null(model0)) fe_terms <- c(fe_terms, "model")
  if (is.null(year0))  fe_terms <- c(fe_terms, "factor(year)")
  if (is.null(trim0))  fe_terms <- c(fe_terms, "trim")
  
  if (length(fe_terms) > 0) {
    fe_formula <- as.formula(paste("~", paste(fe_terms, collapse = " + ")))
    cat("Using baseline covariates with formula:\n  ", deparse(fe_formula), "\n")
    
    Z <- model.matrix(fe_formula, data = car_df)
    
    # drop intercept column
    if (ncol(Z) > 1) {
      Z <- Z[, -1, drop = FALSE]
      use_covs <- TRUE
      cat("Number of baseline covariate columns: ", ncol(Z), "\n\n")
    } else {
      # only intercept; effectively no covariates
      Z <- NULL
      use_covs <- FALSE
      cat("Only intercept in baseline covariates; treating as no covariate adjustment.\n\n")
    }
  } else {
    fe_formula <- NULL
    Z <- NULL
    use_covs <- FALSE
    cat("All of make/model/year/trim fixed. No covariate adjustment used.\n\n")
  }
  
  # helper wrapper to call rdrobust with or without covs
  rd_with_covs <- function(y, x, c, p, kernel, bwselect = "mserd", h = NULL, all = NULL) {
    common_args <- list(y = y, x = x, c = c, p = p, kernel = kernel, bwselect = bwselect)
    if (!is.null(h))   common_args$h   <- h
    if (!is.null(all)) common_args$all <- all
    
    if (use_covs) {
      common_args$covs <- Z
    }
    do.call(rdrobust, common_args)
  }
  
  ## 4. DESCRIPTIVE PLOTS ----
  cat("==== 4. DESCRIPTIVE PLOTS ====\n")
  
  label_vec <- c(
    if (is.null(make0))  "All makes"  else make0,
    if (is.null(model0)) "All models" else model0,
    if (is.null(year0))  "All years"  else as.character(year0),
    if (is.null(trim0))  "All trims"  else trim0
  )
  car_label <- paste(label_vec, collapse = " ")
  
  p_raw_price <- ggplot(car_df, aes(x = mileage, y = price)) +
    geom_point(alpha = 0.3) +
    geom_vline(xintercept = cutoff_miles, linetype = "dashed") +
    labs(
      title = paste("Raw price vs mileage:", car_label),
      x = "Mileage",
      y = "Asking price (USD)"
    )
  
  p_raw_log <- ggplot(car_df, aes(x = mileage, y = log_price)) +
    geom_point(alpha = 0.3) +
    geom_vline(xintercept = cutoff_miles, linetype = "dashed") +
    labs(
      title = paste("log(price) vs mileage:", car_label),
      x = "Mileage",
      y = "log(price)"
    )
  
  p_hist_mileage <- ggplot(car_df, aes(x = mileage_centered)) +
    geom_histogram(bins = 60) +
    geom_vline(xintercept = 0, linetype = "dashed") +
    labs(
      title = "Distribution of mileage around cutoff",
      x = paste0("Mileage - ", cutoff_miles),
      y = "Count"
    )
  
  print(p_raw_price)
  print(p_raw_log)
  print(p_hist_mileage)
  cat("Descriptive plots printed.\n\n")
  
  ## 5. MAIN RDD ESTIMATES (log price & price) ----
  cat("==== 5. MAIN SHARP RDD ESTIMATES ====\n")
  cat("5.1 RDD on log(price)\n")
  
  rd_log_main <- rd_with_covs(
    y = car_df$log_price,
    x = car_df$mileage,
    c = cutoff_miles,
    p = 1,
    kernel = "triangular",
    bwselect = "mserd",
    all = "i"
  )
  print(summary(rd_log_main))
  cat("\n")
  
  cat("5.2 RDD on price levels\n")
  rd_price_main <- rd_with_covs(
    y = car_df$price,
    x = car_df$mileage,
    c = cutoff_miles,
    p = 1,
    kernel = "triangular",
    bwselect = "mserd",
    all = "i"
  )
  print(summary(rd_price_main))
  cat("\n")
  
  ## 6. RDD PLOTS (rdplot) ----
  cat("==== 6. RDD PLOTS (rdplot) ====\n")
  
  rdplot_log <- rdplot(
    y = car_df$log_price,
    x = car_df$mileage,
    c = cutoff_miles,
    title   = paste("RDD plot: log(price) vs mileage around", cutoff_miles),
    x.label = "Mileage",
    y.label = "log(price)"
  )
  
  rdplot_price <- rdplot(
    y = car_df$price,
    x = car_df$mileage,
    c = cutoff_miles,
    title   = paste("RDD plot: price vs mileage around", cutoff_miles),
    x.label = "Mileage",
    y.label = "Price"
  )
  
  cat("rdplot objects created (printed in plotting window).\n\n")
  
  cat("====================================\n")
  cat("   RDD PIPELINE COMPLETE\n")
  cat("====================================\n\n")
  
  # Return key objects for later use
  invisible(list(
    data              = car_df,
    covariate_formula = fe_formula,
    covariates_matrix = Z,
    rd_log_main       = rd_log_main,
    rd_price_main     = rd_price_main,
    plots = list(
      raw_price     = p_raw_price,
      raw_log       = p_raw_log,
      hist_mileage  = p_hist_mileage
    ),
    rdplots = list(
      log_price  = rdplot_log,
      price      = rdplot_price
    )
  ))
}
result_ram_2019_limited <- run_rdd_for_vehicle(
  make0  = "Ram",
  model0 = "1500",
  year0  = 2019L,
  trim0  = "Limited",
  cutoff_miles = 100000,
  max_window   = 50000
)
result_jeep_limited <- run_rdd_for_vehicle(
  make0  = "Jeep",
  model0 = "Grand Cherokee",
  year0  = 2015L,
  trim0  = "Limited",
  cutoff_miles = 100000,
  max_window   = 50000
)
result_ram_2019_limited_60k <- run_rdd_for_vehicle(
  make0  = "Ram",
  model0 = "1500",
  year0  = 2019L,
  trim0  = "Limited",
  cutoff_miles = 60000,
  max_window   = 20000
)
result_jeep_limited_105k <- run_rdd_for_vehicle(
  make0  = "Jeep",
  model0 = "Grand Cherokee",
  year0  = 2015L,
  trim0  = "Limited",
  cutoff_miles = 105000,
  max_window   = 50000
)
result_pooled_all <- run_rdd_for_vehicle(
  make0  = NULL,
  model0 = NULL,
  year0  = NULL,
  trim0  = NULL,
  cutoff_miles = 100000,
  max_window   = 5000
)
result_pooled_all_60k <- run_rdd_for_vehicle(
  make0  = NULL,
  model0 = NULL,
  year0  = NULL,
  trim0  = NULL,
  cutoff_miles = 60000,
  max_window   = 5000
)

result_ram_2019_limited_60k <- run_rdd_for_vehicle(
  make0  = "Ram",
  model0 = "1500",
  year0  = 2019L,
  trim0  = "Limited",
  cutoff_miles = 60000,
  max_window   = 50000
)