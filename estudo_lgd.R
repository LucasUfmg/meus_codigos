
# Libs --------------------------------------------------------------------
install.packages("RJDBC")
library(tidyverse)
library(arrow)
library(data.table)
library(readxl)
library(RJDBC)
library(DBI)
library(lubridate)


# Le 3040 -----------------------------------------------------------------

path_cp66 <- function(mes,ano) {
  
  mes <- str_pad(mes,2,pad = "0")
  path <- paste0("/Volumes/DATA_ANALYTICS/00. Base de Dados/Riscos/cp66_pqt/",
                 ano, "/", mes, "/cp66.paruqet")
  return(path)
}

p <- map2_chr(c(rep(1:12,7)), rep(2015:2021, each = 12), path_cp66) # gostei por 
# ter mais controle que list.files(), mas list.files(x, full.names = T) traria
# todos os dados tambem

# traz parquet na estrtura desejada

pqt_cp66 <- function(x){
  
  bd <- read_parquet(x,
                     col_select = c("mod", "contrato", "data", "cpf_cnpj",
                                    "saldo_devedor", "carteira", "atraso",
                                    "prazo"))
  setDT(bd)
  
  bd <- bd[mod %in% c("0211","0901","0902","0903","0990")] # Imobiliario
  
  print(x)
  
  return(bd)
  
}

lcp66 <- lapply(p, pqt_cp66) # Traz ate 2021

saveRDS(lcp66, "/Users/bi005271/lgd/cp66_LGD.RDS")


# Le base de consolidacao -------------------------------------------------

# Consolidacao espalhada em bases diferentes
bd_con <- read_excel("planilhao (1).xlsx") %>% setDT()
vendas <- read_excel("Base venda .xlsx") %>% setDT()
bnup <- read_excel("Base geral.xlsx") %>% setDT()

# Base bd_con com contratos apenas
# Unifica os contratos
bd_con_sub <- bd_con[, .(`NOME DEVEDOR` = NOME, `NUMERO DO CONTRATO` = CONTRATO)]
vendas_sub <- vendas[, .(`NOME DEVEDOR`, `NUMERO DO CONTRATO`)]
bnup_sub <- bnup[, .(`NOME DEVEDOR`, `NUMERO DO CONTRATO`)]

bd_contratos <- rbindlist(list(bd_con_sub, vendas_sub, bnup_sub))

bd_contratos_dist <- distinct(bd_consolidacao)

# Completa base de consolidacao

bd_contratos_dist[bd_con,
                  on = c("NOME DEVEDOR", "NUMERO DO CONTRATO")][vendas_sub, on = c("NOME DEVEDOR", "NUMERO DO CONTRATO")][bnup_sub, on = c("NOME DEVEDOR", "NUMERO DO CONTRATO")]


bd_contratos_dist2 = bd_contratos_dist[,filter(`TIPO ENTRADA` == "Consolicação")] # 1355 contratos (os outros sao Dacao)



# Le DM_CI ----------------------------------------------------------------

# Conexao com Servidor
bi <- rstudioapi::askForPassword("DIGITE SEU BI")

drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver", str_glue(
  
  "/Users/{bi}/Library/DBeaverData/drivers/maven/maven-central/com.microsoft.sqlserver/mssql-jdbc-9.2.0.jre8.jar"))

interclue1110sql <- dbConnect(
  
  drv = drv,
  url = paste0(
    "jdbc:sqlserver://interclu1110sql.intermedium.local;",
    "integratedSecurity=true;",
    "encrypt=true;",
    "authenticationScheme=NTLM;",
    "trustServerCertificate=true;",
    "ApplicationIntent=ReadOnly"
  ),
  user = bi,
  password = rstudioapi::askForPassword("Senha")
)

# Query de requisicao dos dados

dm_ci <- dbGetQuery(interclue1110sql, "SELECT
                    A.CD_CONTRATOCORRELACIONAL,
                    .
                    .
                    .
                    FROM DM_CI.dbo.FATO_CONTRATO A INNER JOIN
                    DM_CI.dbo.dim_CONTRATO B ON A.SK_CONTRATO = B.SK_CONTRATO INNER JOIN
                    DM_CI.dbo.DIM_TEMPO_DIA D ON A.SK_DTLIBERACAO = D.SK_TEMPO_DIA INNER JOIN
                    DM_CI.dbo.DIM_CLIENTE C ON A.SK_CLIENTE = C.SK_CLIENTE
                    ")

setDT(dm_ci)

# Observacoes 'mais atualizadas'por contrato

dm_ci[, dt_lib_max:= max(DT_DATA), CD_CONTRATO_DIGITO]

dm_ci <- dm_ci[dt_lib_max == DT_DATA, !c("dt_lib_max","DT_DATA")] # Remove dt_lib_max e DT_DATA?


# Contratos em inadimplencia ----------------------------------------------

cp66 <- setorder(cp66,contrato,data)
cp66[, atraso_lag:= lag(atraso), contrato] # Lag para atrasos em default
cp66[, default:= ifelse(atraso >= 90 & atraso_lag < 90, 1, 0)]

cp66_min <- cp66[default == 1, data_min:= min(data), contrato][data == data_min] # Define data minima
cp66_max <- cp66[default == 1, data_max:= max(data), contrato][data == data_max] # Define data maxima

# Cria cp66 com data max e data min
default <- cp66 %>%
  group_by(contrato) %>%
  mutate(data_min =  min(data), data_max = max(data)) %>%
  dplyr::select(contrato,mod,saldo_default_inicial,
                saldo_default_final, data_default_final, data_default_inicial,
                atraso_default_inicial, atraso_default_final) %>%
  mutate(n_default = sum(default)) %>%
  dplyr::select(mod,contrato,saldo_default_inicial,
                saldo_default_final, data_default_final, data_default_inicial,
                atraso_default_inicial, atraso_default_final, n_default)



# Data fusion -------------------------------------------------------------

dm_ci %>%
  mutate(CD_CONTRATO_DIGITO = as.character(CD_CONTRATO_DIGITO)) %>%
  right_join(default, all.y = T, by.x = "contrato", by.y = "CD_CONTRATO_DIGITO") %>%
  setDT()


















