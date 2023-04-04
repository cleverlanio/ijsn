############################################
#Script da PNADC TRIMESTRAL EDUCACAO
#Tema: Educação     
#Autor: Rafael Correia das Neves
#email: rafael.neves@ijsn.es.gov.br
############################################

# BIBLIOTECAS ####
library(foreign);
library(sqldf);
library(data.table);
library(ijsn);
library(plyr);

# TODO - RECEBER ESSE VETOR VIA PARAMETROS DE INICIALIZACAO #####


args = commandArgs(TRUE)
if(length(args) == 0) {
  path = sys.frame(1)$ofile
  dirinput = dirname(path)
  scriptfile = basename(path)
} else {
  dirinput = args[1]
  scriptfile = args[2]
}

filenamebase = substr(scriptfile, 1, regexpr(".R", scriptfile, fixed=T)[1] - 1)
pathinput = paste0(dirinput, '/', filenamebase, '_input.csv')
anotrim = fread(pathinput)$ANOTRIM
anotrim = sort(anotrim, decreasing = TRUE)

options(sqldf.driver = "SQLite")

inicio = Sys.time()

for (i in 1:length(anotrim)){
  
  # LEITURA DO ARQUIVO DE ENTRADA ####
  
  # LER O ARQUIVO DE ENTRADA
  # PASSAR O DIRETORIO DE LEITURA VIA PARAMETRO DE INICIALIZACAO
  
  print(anotrim[i])  
  
  if (!anotrim[i] %in% c("2012T1", "2012T2", "2012T3", "2012T4", "2013T1", "2013T2",
                         "2013T3", "2013T4", "2014T1", "2014T2", "2014T3", "2014T4",
                         "2015T1", "2015T2", "2015T3")) {
    
    pesTEMP = getDataFile("pesquisa-nacional-por-amostra-de-domicilios-continua",
                          paste0("PNADC ", anotrim[i]),
                          paste0("PNADC_", anotrim[i]),
                          c('Ano','Trimestre','UF','Capital','RM_RIDE','V1023','UPA',
                            'V1008','V1014','V2005','V2007','V2008','V20081','V20082','V2009', 'V2010',
                            'V1028','V3001','V3002','V3003A','V3004','V3006','V4071','V4074A',
                            'VD3004','VD4001','VD4002','VD4019'))
    
    #Este "de para" não deve ser usado em outros scripts. Posteriormente os códigos
    #serão agregados de modo que a imprecisão na tradução não seja relevante
    pesTEMP$V3003A <- mapvalues(pesTEMP$V3003A,
                                from = c(1,2,3,4,5,6,7,8,9,10,11),
                                to   = c(1,1,2,3,4,5,6,7,7, 8, 9),
                                warn_missing = FALSE)
    
    #Este "de para" não deve ser usado em outros scripts. Posteriormente os códigos
    #serão agregados de modo que a imprecisão na tradução não seja relevante
    pesTEMP$V4074A <- mapvalues(pesTEMP$V4074A,
                                from = c(1,2,3,4,5,6,7,8,9,10),
                                to   = c(1,2,3,3,4,3,5,6,7, 8),
                                warn_missing = FALSE)
    
    setnames(pesTEMP,
             c('V3003A','V4074A'),
             c('V3003' ,'V4074'))
    
    #Esta variavel indica que todos que frequentam escola, frequentam o ensino de 9 anos
    pesTEMP$V3004 <- ifelse(pesTEMP$V3002==1, 2, pesTEMP$V3004)
    
    
  } else {
    
    pesTEMP = getDataFile("pesquisa-nacional-por-amostra-de-domicilios-continua",
                          paste0("PNADC ", anotrim[i]),
                          paste0("PNADC_", anotrim[i]),
                          c('Ano','Trimestre','UF','Capital','RM_RIDE','V1023','UPA',
                            'V1008','V1014','V2005','V2007','V2008','V20081','V20082','V2009', 'V2010',
                            'V1028','V3001','V3002','V3003','V3004','V3006','V4071','V4074',
                            'VD3004','VD4001','VD4002','VD4019'))
  }
  
  
  ####### INICIO - CRIACAO VARIAVEL CATEGORICA REGISTRO BRUTO #########
  
  # COMEÇO DA CRIAÇÃO DA VARIÁVEL DE RENDA DO TRABALHO PER CAPITA#
  # CRIA UMA NOVA VARIÁVEL DE NIVEL GEOGRÁFICO QUE CONTÉM INTERIOR, CAPITAL E RM.
  
  # 1º Passo, criar uma chave de domicílio que considere a condição da pessoa no
  # domicílio
  
  pesTEMP = sqldf(removeSQLComments("
                                    SELECT
                                    *,
                                    CASE
                                    WHEN Capital IS NOT NULL THEN UF * 10 #Capital#
                                    WHEN (UF = RM_RIDE) AND V1023 = 2 THEN (UF * 10 + 1) #Reg. Metr. (-) Capital  #
                                    ELSE UF * 10 + 3 #Interior. Recebe código 3 pq 2 vai ser região metrop#
                                    END AS CAT_NIV_GEO,
                                    
                                    CASE
                                    #Casos normais#
                                    WHEN V2005 IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,16) THEN 0
                                    #Agregado que não compartilha despesa#
                                    WHEN V2005 = 15 THEN 1
                                    #Pensionista#
                                    WHEN V2005 = 17 THEN 2
                                    #Empregado e parente de empregado doméstico#
                                    WHEN V2005 IN (18,19) THEN 3
                                    END AS COND_DOM
                                    FROM pesTEMP
                                    "))
  
  setDT(pesTEMP)
  
  pesTEMP$npess <- 1
  
  pesTEMP2 = pesTEMP[,
                     list(
                       npes_dom = sum(npess, na.rm = TRUE),
                       rdt = sum(VD4019, na.rm = TRUE)
                     ),
                     by=list(
                       Ano,
                       Trimestre,
                       UF,
                       UPA,
                       V1008,
                       V1014,
                       COND_DOM
                     )
                     ]
  
  pesTEMP3 = merge(pesTEMP, pesTEMP2, by=c('Ano','Trimestre','UF','UPA','V1008','V1014','COND_DOM'), allow.cartesian=TRUE)
  
  pesTEMP3$rdtpc <- (pesTEMP3$rdt/pesTEMP3$npes_dom)
  pesTEMP3$rdtpc <- ifelse(pesTEMP3$rdtpc == 0, NA, pesTEMP3$rdtpc)
  
  pesTEMP <- pesTEMP3
  
  rm(pesTEMP2)
  rm(pesTEMP3)
  
  ############################################################
  #FIM DA CRIAÇÃO DA VARIÁVEL DE RENDA DO TRABALHO PER CAPITA#
  ############################################################
  
  ##  Variáveis originais que não são mais necessárias a partir daqui:
  ##  V1008, UPA, V1014, VD4019
  ##  Variáveis derivadas que não são mais necessárias a partir daqui:
  ##  COND_DOM, rdt, npes_dom,
  
  ###############################################################
  # GERA DATAFRAME COM A POPULACAO (COM ALGUMA RENDA DO TRABALHO)
  # POR UF, REGIAO E BRASIL DIVIDIDA POR 5
  ###############################################################
  popDIV5 = sqldf(removeSQLComments("
                                    SELECT
                                    a.Ano,
                                    a.Trimestre,
                                    a.UF,
                                    a.TOTPOPUF/5 AS TOTPOPDIV5UF,
                                    b.TOTPOPREGIAO/5 AS TOTPOPDIV5REGIAO,
                                    c.TOTPOPBRASIL/5 AS TOTPOPDIV5BRASIL
                                    FROM
                                    (SELECT
                                    Ano,
                                    Trimestre,
                                    UF,
                                    ROUND(UF/10) AS REGIAO,
                                    0 AS BRASIL,
                                    SUM(V1028) AS TOTPOPUF
                                    FROM pesTEMP
                                    WHERE rdtpc IS NOT NULL
                                    AND rdtpc > 0
                                    GROUP BY
                                    Ano,
                                    Trimestre,
                                    UF
                                    ) a,
                                    (SELECT
                                    Ano,
                                    Trimestre,
                                    ROUND(UF/10) AS REGIAO,
                                    SUM(V1028) AS TOTPOPREGIAO
                                    FROM pesTEMP
                                    WHERE rdtpc IS NOT NULL
                                    AND rdtpc > 0
                                    GROUP BY
                                    Ano,
                                    Trimestre,
                                    UF/10
                                    ) b,
                                    (SELECT
                                    Ano,
                                    Trimestre,
                                    0 AS BRASIL,
                                    SUM(V1028) AS TOTPOPBRASIL
                                    FROM pesTEMP
                                    WHERE rdtpc IS NOT NULL
                                    AND rdtpc > 0
                                    GROUP BY
                                    Ano,
                                    Trimestre
                                    ) c
                                    WHERE a.Ano = b.Ano
                                    AND a.Ano = c.Ano
                                    AND a.Trimestre = b.Trimestre
                                    AND a.Trimestre = c.Trimestre
                                    AND a.REGIAO = b.REGIAO
                                    AND a.BRASIL = c.BRASIL
                                    ORDER BY
                                    a.Ano,
                                    a.Trimestre,
                                    a.UF
                                    "))
  
  
  ###################################################
  # CRIA CAMPOS DE RENDA TRATADO, BASE DA ORDENACAO,#
  # E DE POPULACAO, BASE DA ACUMULACAO              #
  ###################################################
  pesTEMP = sqldf("
                  SELECT
                  *,
                  CASE
                  WHEN rdtpc IS NULL THEN 0.0
                  WHEN rdtpc > 0 THEN rdtpc
                  ELSE 0.0
                  END AS REM_HAB_PRE_QUINTIL,
                  CASE
                  WHEN rdtpc IS NULL THEN 0.0
                  WHEN rdtpc > 0 THEN V1028
                  ELSE 0.0
                  END AS POP_PRE_QUINTIL
                  
                  FROM pesTEMP
                  ")
  
  #####################################################################
  # REALIZA SOMA ACUMULADA POR UF DA POPULACAO TRATADA
  #####################################################################
  setDT(pesTEMP)
  
  pesTEMP = pesTEMP[order(UF, REM_HAB_PRE_QUINTIL)]
  
  pesTEMP = pesTEMP[,
                    list(
                      Ano,
                      Trimestre,
                      UF,
                      CAT_NIV_GEO,
                      V2007,
                      V2008,
                      V20081,
                      V20082,
                      V2009,
                      V2010,
                      V1028,
                      V3001,
                      V3002,
                      V3003,
                      V3004,
                      V3006,
                      V4071,
                      V4074,
                      VD3004,
                      VD4001,
                      VD4002,
                      rdtpc,
                      REM_HAB_PRE_QUINTIL,
                      POP_PRE_QUINTIL,
                      V1028_CUMSUM_UF=cumsum(POP_PRE_QUINTIL)
                    ),
                    by=UF
                    ]
  
  #####################################################################
  # REALIZA SOMA ACUMULADA POR REGIAO DA POPULACAO TRATADA
  #####################################################################
  pesTEMP = pesTEMP[order(floor(UF/10), REM_HAB_PRE_QUINTIL)]
  
  pesTEMP = pesTEMP[,
                    list(
                      Ano,
                      Trimestre,
                      UF,
                      CAT_NIV_GEO,
                      V2007,
                      V2008,
                      V20081,
                      V20082,
                      V2009,
                      V2010,
                      V1028,
                      V3001,
                      V3002,
                      V3003,
                      V3004,
                      V3006,
                      V4071,
                      V4074,
                      VD3004,
                      VD4001,
                      VD4002,
                      rdtpc,
                      REM_HAB_PRE_QUINTIL,
                      POP_PRE_QUINTIL,
                      V1028_CUMSUM_UF,
                      V1028_CUMSUM_REGIAO=cumsum(POP_PRE_QUINTIL)
                    ),
                    by=list(REGIAO=floor(UF/10))
                    ]
  
  ####################################################
  # REALIZA SOMA ACUMULADA BRASIL DA POPULACAO TRATADA
  ####################################################
  pesTEMP = pesTEMP[order(REM_HAB_PRE_QUINTIL)]
  
  pesTEMP = pesTEMP[,
                    list(
                      Ano,
                      Trimestre,
                      UF,
                      CAT_NIV_GEO,
                      V2007,
                      V2008,
                      V20081,
                      V20082,
                      V2009,
                      V2010,
                      V1028,
                      V3001,
                      V3002,
                      V3003,
                      V3004,
                      V3006,
                      V4071,
                      V4074,
                      VD3004,
                      VD4001,
                      VD4002,
                      rdtpc,
                      V1028_CUMSUM_UF,
                      V1028_CUMSUM_REGIAO,
                      V1028_CUMSUM_BRASIL=cumsum(POP_PRE_QUINTIL)
                    ),
                    by=list(BRASIL=(UF-UF))
                    ]
  
  ########################################################################
  # CRIAR VARIAVEL CATEGORICA QUINTIL RENDA HABITUAL POR UF, REGIAO E BRASIL
  ########################################################################
  pesTEMP = sqldf(removeSQLComments("
                                    SELECT
                                    a.Ano,
                                    a.Trimestre,
                                    a.UF,
                                    CAT_NIV_GEO,
                                    V2007,
                                    V2008,
                                    V20081,
                                    V20082,
                                    V2009,
                                    V2010,
                                    V1028,
                                    V3001,
                                    V3002,
                                    V3003,
                                    V3004,
                                    V3006,
                                    V4071,
                                    V4074,
                                    VD3004,
                                    VD4001,
                                    VD4002,
                                    rdtpc,
                                    V1028_CUMSUM_UF,
                                    V1028_CUMSUM_REGIAO,
                                    V1028_CUMSUM_BRASIL,
                                    CASE
                                    WHEN rdtpc IS NULL THEN NULL
                                    WHEN V1028_CUMSUM_UF <=    TOTPOPDIV5UF THEN 1
                                    WHEN V1028_CUMSUM_UF >     TOTPOPDIV5UF AND V1028_CUMSUM_UF <= 2 * TOTPOPDIV5UF THEN 2
                                    WHEN V1028_CUMSUM_UF > 2 * TOTPOPDIV5UF AND V1028_CUMSUM_UF <= 3 * TOTPOPDIV5UF THEN 3
                                    WHEN V1028_CUMSUM_UF > 3 * TOTPOPDIV5UF AND V1028_CUMSUM_UF <= 4 * TOTPOPDIV5UF THEN 4
                                    WHEN V1028_CUMSUM_UF > 4 * TOTPOPDIV5UF THEN 5
                                    ELSE NULL
                                    END AS CAT_REM_HAB_QUINTIL_UF,
                                    CASE
                                    WHEN rdtpc IS NULL THEN NULL
                                    WHEN V1028_CUMSUM_REGIAO <=    TOTPOPDIV5REGIAO THEN 1
                                    WHEN V1028_CUMSUM_REGIAO >     TOTPOPDIV5REGIAO AND V1028_CUMSUM_REGIAO <= 2 * TOTPOPDIV5REGIAO THEN 2
                                    WHEN V1028_CUMSUM_REGIAO > 2 * TOTPOPDIV5REGIAO AND V1028_CUMSUM_REGIAO <= 3 * TOTPOPDIV5REGIAO THEN 3
                                    WHEN V1028_CUMSUM_REGIAO > 3 * TOTPOPDIV5REGIAO AND V1028_CUMSUM_REGIAO <= 4 * TOTPOPDIV5REGIAO THEN 4
                                    WHEN V1028_CUMSUM_REGIAO > 4 * TOTPOPDIV5REGIAO THEN 5
                                    ELSE NULL
                                    END AS CAT_REM_HAB_QUINTIL_REGIAO,
                                    CASE
                                    WHEN rdtpc IS NULL THEN NULL
                                    WHEN V1028_CUMSUM_BRASIL <=    TOTPOPDIV5BRASIL THEN 1
                                    WHEN V1028_CUMSUM_BRASIL >     TOTPOPDIV5BRASIL AND V1028_CUMSUM_BRASIL <= 2 * TOTPOPDIV5BRASIL THEN 2
                                    WHEN V1028_CUMSUM_BRASIL > 2 * TOTPOPDIV5BRASIL AND V1028_CUMSUM_BRASIL <= 3 * TOTPOPDIV5BRASIL THEN 3
                                    WHEN V1028_CUMSUM_BRASIL > 3 * TOTPOPDIV5BRASIL AND V1028_CUMSUM_BRASIL <= 4 * TOTPOPDIV5BRASIL THEN 4
                                    WHEN V1028_CUMSUM_BRASIL > 4 * TOTPOPDIV5BRASIL THEN 5
                                    ELSE NULL
                                    END AS CAT_REM_HAB_QUINTIL_BRASIL
                                    
                                    FROM pesTEMP a, popDIV5 b
                                    
                                    WHERE a.Ano = b.Ano
                                    AND a.Trimestre = b.Trimestre
                                    AND a.UF = b.UF
                                    "))
  
  #Elimina vars desnecessárias
  pesTEMP$V1028_CUMSUM_BRASIL <- NULL
  pesTEMP$V1028_CUMSUM_REGIAO <- NULL
  pesTEMP$V1028_CUMSUM_UF <- NULL
  pesTEMP$rdtpc <- NULL
  
  ######## FIM - CRIACAO VARIAVEL CATEGORICA REGISTRO BRUTO
  
  
  
  ############ INICIO - CRIACAO VARIAVEIS CATEGORICAS
  
  
  # DIA E MES DE REFERENCIA A SER UTILIZADO NO RECALCULO DA IDADE
  
  # Dia de referencia = 31
  # mes de referencia = 3, veja dentro do SQL
  
  # CACULAR PRIMEIRA LEVA DE VARIAVEIS CATEGORICAS QUE DEPENDEM
  # APENAS DAS VARIAVEIS ORIGINAIS DO ARQUIVO DE ENTRADA
  #
  # CAT_IDADE_REF_31MARCO
  # COMO HOUVE O RECALCULO DA IDADE USANDO A DATA 31/03, PASSOU A EXISTIR A IDADE -1
  # PARA AS PESSOAS QUE NASCERAM ENTRE ABRIL E DATA DA ENTREVISTA DAQUELE ANO
  
  pesTEMP = sqldf("
                  SELECT
                  *,
                  CASE
                  WHEN V2008 = 99 THEN V2009
                  WHEN V20081 = 99 THEN V2009
                  WHEN V20082 = 9999 THEN V2009
                  WHEN V20081 > 3 THEN Ano-V20082-1
                  WHEN V20081 < 3 THEN Ano-V20082
                  WHEN V20081 = 3 AND V2008 > 31 THEN Ano-V20082-1
                  WHEN V20081 = 3 AND V2008 <= 31 THEN Ano-V20082
                  ELSE NULL
                  END AS CAT_IDADE_REF_31MARCO,
                  CASE
                  WHEN V2008 = 99 THEN NULL
                  WHEN V20081 = 99 THEN NULL
                  WHEN V20082 = 9999 THEN NULL
                  WHEN V20081 > 3 THEN Ano-V20082-1
                  WHEN V20081 < 3 THEN Ano-V20082
                  WHEN V20081 = 3 AND V2008 > 31 THEN Ano-V20082-1
                  WHEN V20081 = 3 AND V2008 <= 31 THEN Ano-V20082
                  ELSE NULL
                  END AS CAT_IDADE_REF_31MARCO_NO_INPUT
                  
                  FROM pesTEMP
                  ")
  
  # Elimina mais variáveis desnecessárias
  
  pesTEMP$V2008 <- NULL
  pesTEMP$V20082 <- NULL
  pesTEMP$V20081 <- NULL
  
  
  #####################################################################
  # TRANSFORMAR OS CAMPOS NOVOS EM NUMERICOS
  #####################################################################
  pesTEMP$CAT_IDADE_REF_31MARCO = as.numeric(pesTEMP$CAT_IDADE_REF_31MARCO)
  pesTEMP$CAT_IDADE_REF_31MARCO_NO_INPUT = as.numeric(pesTEMP$CAT_IDADE_REF_31MARCO_NO_INPUT)
  
  
  ################################################################################## 
  # CACULAR SEGUNDA LEVA DE VARIAVEIS CATEGORICAS QUE DEPENDEM
  # DAS VARIAVEIS ORIGINAIS DO ARQUIVO DE ENTRADA E DAS CATEGORICAS
  # DA PRIMEIRA LEVA
  #
  # CAT_FX_ETARIA_REF_31MARCO
  # COMO HOUVE O RECALCULO DA IDADE USANDO A DATA 31/03, PASSOU A EXISTIR A IDADE -1
  # PARA AS PESSOAS QUE NASCERAM ENTRE ABRIL E DATA DA ENTREVISTA DAQUELE ANO
  ##################################################################################
  pesTEMP = sqldf(removeSQLComments("
                                    SELECT
                                    *,
                                    CASE
                                    WHEN V2009 >= 0 AND V2009 <= 4 THEN '00 a 04 Anos'
                                    WHEN V2009 = 5 THEN '05 Anos'
                                    WHEN V2009 = 6 THEN '06 Anos'
                                    WHEN V2009 = 7 THEN '07 Anos'
                                    WHEN V2009 = 8 THEN '08 Anos'
                                    WHEN V2009 = 9 THEN '09 Anos'
                                    WHEN V2009 = 10 THEN '10 Anos'
                                    WHEN V2009 = 11 THEN '11 Anos'
                                    WHEN V2009 = 12 THEN '12 Anos'
                                    WHEN V2009 = 13 THEN '13 Anos'
                                    WHEN V2009 = 14 THEN '14 Anos'
                                    WHEN V2009 = 15 THEN '15 Anos'
                                    WHEN V2009 = 16 THEN '16 Anos'
                                    WHEN V2009 = 17 THEN '17 Anos'
                                    WHEN V2009 = 18 THEN '18 Anos'
                                    WHEN V2009 = 19 THEN '19 Anos'
                                    WHEN V2009 >= 20 AND V2009 <= 24 THEN '20 a 24 Anos'
                                    WHEN V2009 >= 25 AND V2009 <= 29 THEN '25 a 29 Anos'
                                    WHEN V2009 >= 30 THEN '30 Anos ou Mais'
                                    ELSE NULL
                                    END AS CAT_FX_ETARIA,
                                    
                                    CASE
                                    WHEN V2009 >= 0 AND V2009 <= 14 THEN '00 a 14 Anos'
                                    WHEN V2009 >= 15 AND V2009 <= 17 THEN '15 a 17 Anos'
                                    WHEN V2009 >= 18 AND V2009 <= 24 THEN '18 a 24 Anos'
                                    WHEN V2009 >= 25 AND V2009 <= 29 THEN '25 a 29 Anos'
                                    WHEN V2009 >= 30 AND V2009 <= 39 THEN '30 a 39 Anos'
									WHEN V2009 >= 40 AND V2009 <= 49 THEN '40 a 49 Anos'
									WHEN V2009 >= 50 AND V2009 <= 59 THEN '50 a 59 Anos'
									WHEN V2009 >= 60 THEN '60 Anos ou mais'
                                    ELSE NULL
                                    END AS CAT_FX_ETARIA_JUV,
                                    
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO >= -1 AND CAT_IDADE_REF_31MARCO <= 4 THEN '00 a 04 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO = 5 THEN '05 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO = 6 THEN '06 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO = 7 THEN '07 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO = 8 THEN '08 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO = 9 THEN '09 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO = 10 THEN '10 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO = 11 THEN '11 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO = 12 THEN '12 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO = 13 THEN '13 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO = 14 THEN '14 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO = 15 THEN '15 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO = 16 THEN '16 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO = 17 THEN '17 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO = 18 THEN '18 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO = 19 THEN '19 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO >= 20 AND CAT_IDADE_REF_31MARCO <= 24 THEN '20 a 24 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO >= 25 AND CAT_IDADE_REF_31MARCO <= 29 THEN '25 a 29 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO >= 30 THEN '30 Anos ou Mais'
                                    ELSE NULL
                                    END AS CAT_FX_ETARIA_REF_31MARCO,
                                    
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO_NO_INPUT >= -1 AND CAT_IDADE_REF_31MARCO_NO_INPUT <= 4 THEN '00 a 04 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO_NO_INPUT = 5 THEN '05 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO_NO_INPUT = 6 THEN '06 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO_NO_INPUT = 7 THEN '07 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO_NO_INPUT = 8 THEN '08 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO_NO_INPUT = 9 THEN '09 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO_NO_INPUT = 10 THEN '10 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO_NO_INPUT = 11 THEN '11 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO_NO_INPUT = 12 THEN '12 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO_NO_INPUT = 13 THEN '13 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO_NO_INPUT = 14 THEN '14 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO_NO_INPUT = 15 THEN '15 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO_NO_INPUT = 16 THEN '16 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO_NO_INPUT = 17 THEN '17 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO_NO_INPUT = 18 THEN '18 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO_NO_INPUT = 19 THEN '19 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO_NO_INPUT >= 20 AND CAT_IDADE_REF_31MARCO_NO_INPUT <= 24 THEN '20 a 24 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO_NO_INPUT >= 25 AND CAT_IDADE_REF_31MARCO_NO_INPUT <= 29 THEN '25 a 29 Anos'
                                    WHEN CAT_IDADE_REF_31MARCO >= 30 THEN '30 Anos ou Mais'
                                    ELSE NULL
                                    END AS CAT_FX_ETARIA_REF_31MARCO_NO_INPUT,
                                    
                                    #
                                    A - Não frequenta escola e não concluiu o ensino fundamental
                                    B - Frequenta o ensino fundamental
                                    C - Concluiu o ensino fundamental e frequenta o ensino médio
                                    D - Concluiu o ensino fundamental e não frequenta o ensino médio
                                    #
                                    
                                    V2010 AS CAT_COR_RACA,
                                    
                                    CASE
                                    WHEN V3002 = 1 AND VD3004 IN (1) THEN 'A'
                                    WHEN V3002 = 2 AND VD3004 IN (1,2) THEN 'A'
                                    WHEN V3002 = 1 AND VD3004 IN (2) THEN 'B'
                                    WHEN V3002 = 1 AND VD3004 IN (3,4,5,6,7) THEN 'C'
                                    WHEN V3002 = 2 AND VD3004 IN (4,5,6,7) THEN 'C'
                                    WHEN V3002 = 2 AND VD3004 IN (3) THEN 'D'
                                    ELSE NULL
                                    END AS CAT_STATUS_EF,
                                    
                                    #
                                    A - Não frequenta escola e não concluiu o ensino médio
                                    B - Frequenta o ensino médio
                                    C - Concluiu o ensino médio e frequenta o ensino superior
                                    D - Concluiu o ensino médio e não frequenta o ensino superior
                                    #
                                    
                                    CASE
                                    WHEN V3002 = 1 AND VD3004 IN (1,2) THEN 'A'
                                    WHEN V3002 = 2 AND VD3004 IN (1,2,3,4) THEN 'A'
                                    WHEN V3002 = 1 AND VD3004 IN (3,4) THEN 'B'
                                    WHEN V3002 = 1 AND VD3004 IN (5,6,7) THEN 'C'
                                    WHEN V3002 = 2 AND VD3004 IN (6,7) THEN 'C'
                                    WHEN V3002 = 2 AND VD3004 IN (5) THEN 'D'
                                    ELSE NULL
                                    END AS CAT_STATUS_EM,
                                    
                                    #
                                    Cria uma variável que define se a pessoa frequenta educação normal
                                    ou EJA/Classe de alfabetização.
                                    #
                                    
                                    CASE
                                    WHEN V3003 = 1 THEN 'Pré-escola'
                                    WHEN V3003 IN (3,5) THEN 'Educação Básica'
                                    WHEN V3003 IN (7,8,9) THEN 'Educação Superior'
                                    WHEN V3003 IN (2,4,6) THEN 'EJA e Alfabetização'
                                    END AS CAT_TIPO_EDUCA,
                                    
                                    CASE
                                    WHEN V4074 IN (1,2) THEN 'Conseguiu trabalho ou está aguardando resposta'
                                    WHEN V4074 IN (3,4) THEN 'Desistiu de procurar'
                                    WHEN V4074 = 5 THEN 'Tinha que cuidar de filho(s), de outro(s) dependente(s) ou dos afazeres domésticos'
                                    WHEN V4074 = 6 THEN 'Estudo'
                                    WHEN V4074 IN (7,8) THEN 'Outro motivo'
                                    END AS CAT_MOTIVOS
                                    
                                    
                                    FROM pesTEMP
                                    "))
  
  # Elimina variáveis desnecessárias
  pesTEMP$V2009 <- NULL
  pesTEMP$V4074 <- NULL
  
  #####################################################################
  ############ FIM - CRIACAO VARIAVEIS CATEGORICAS ####################
  #####################################################################
  
  # CRIAR VARIAVEL REPRESENTANDO A QUANTIDADE DE AMOSTRAS
  
  setDT(pesTEMP)
  
  pesTEMP = pesTEMP[,SOMA_AMOSTRAS:=1L];
  
  # AGRUPAR O DATA.TABLE PARA ACELERAR A VELOCIDADE DAS ETAPAS A
  # SEGUIR E REDUZIR A MEMORIA UTILIZADA SEM JOGAR INFORMACAO UTIL FORA
  
  setkey(pesTEMP,
         Ano,
         Trimestre,
         UF,
         CAT_NIV_GEO,
         CAT_COR_RACA,
         CAT_FX_ETARIA,
         CAT_FX_ETARIA_JUV,
         CAT_FX_ETARIA_REF_31MARCO,
         CAT_FX_ETARIA_REF_31MARCO_NO_INPUT,
         CAT_IDADE_REF_31MARCO,
         CAT_IDADE_REF_31MARCO_NO_INPUT,
         CAT_MOTIVOS,
         CAT_REM_HAB_QUINTIL_BRASIL,
         CAT_REM_HAB_QUINTIL_REGIAO,
         CAT_REM_HAB_QUINTIL_UF,
         CAT_STATUS_EF,
         CAT_STATUS_EM,
         CAT_TIPO_EDUCA,
         V2007,
         V3001,
         V3002,
         V3003,
         V3004,
         V3006,
         V4071,
         VD3004,
         VD4001,
         VD4002
  )
  
  pesTEMP = pesTEMP[,
                    list(
                      SOMA_AMOSTRAS=sum(SOMA_AMOSTRAS, na.rm = TRUE),
                      SOMA_QT_PESSOAS=sum(V1028, na.rm = TRUE)
                    ),
                    by=list(
                      Ano,
                      Trimestre,
                      UF,
                      CAT_NIV_GEO,
                      CAT_COR_RACA,
                      CAT_FX_ETARIA,
                      CAT_FX_ETARIA_JUV,
                      CAT_FX_ETARIA_REF_31MARCO,
                      CAT_FX_ETARIA_REF_31MARCO_NO_INPUT,
                      CAT_IDADE_REF_31MARCO,
                      CAT_IDADE_REF_31MARCO_NO_INPUT,
                      CAT_MOTIVOS,
                      CAT_REM_HAB_QUINTIL_BRASIL,
                      CAT_REM_HAB_QUINTIL_REGIAO,
                      CAT_REM_HAB_QUINTIL_UF,
                      CAT_STATUS_EF,
                      CAT_STATUS_EM,
                      CAT_TIPO_EDUCA,
                      V2007,
                      V3001,
                      V3002,
                      V3003,
                      V3004,
                      V3006,
                      V4071,
                      VD3004,
                      VD4001,
                      VD4002
                    )
                    ]
  
  
  # INICIO - CRIACAO VARIAVEIS QUANTITATIVAS
  
  
  # CALCULAR DENOMINADORES E NUMERADORES DOS INDICADORES
  
  pesTEMP = sqldf(removeSQLComments("
                                    SELECT 
                                    *,
                                    #Analfabetismo#
                                    CASE
                                    WHEN V3001 IS NULL THEN NULL
                                    WHEN V3001 = 2 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT1_N_SABE_LER,
                                    CASE
                                    WHEN V3001 IS NULL THEN NULL
                                    ELSE SOMA_QT_PESSOAS
                                    END AS DENOM_QT1,
                                    
                                    #Alocação do tempo do jovem (só trabalha; trabalha e estuda; só estuda;
                                    não estuda, não trabalha e não procura emprego; não estuda, não trabalha e procura emprego
                                    
                                    Trabalha e estuda#
                                    CASE
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN VD4001 IS NULL THEN NULL
                                    WHEN VD4002 = 1 AND V3002 = 1 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT2_TRAB_EST,
                                    
                                    #Só trabalha#
                                    CASE
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN VD4001 IS NULL THEN NULL
                                    WHEN VD4002 = 1 AND V3002 = 2 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT2_SO_TRAB,
                                    
                                    #Só estuda#
                                    CASE
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN VD4001 IS NULL THEN NULL
                                    WHEN V3002 = 1 AND VD4001 = 2 THEN SOMA_QT_PESSOAS 
                                    WHEN V3002 = 1 AND VD4002 = 2 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT2_SO_ESTUDA,
                                    
                                    #Não estuda e não trabalha (não estuda E não trab) E (procura OU não procura emprego)#
                                    CASE
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN VD4001 IS NULL THEN NULL
                                    WHEN V3002 = 2 AND VD4001 = 2 THEN SOMA_QT_PESSOAS
                                    WHEN V3002 = 2 AND VD4002 = 2 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT2_NEM_NEM,
                                    
                                    #Não estuda, não trabalha e não procura emprego#
                                    CASE
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN VD4001 IS NULL THEN NULL
                                    WHEN V3002 = 2 AND VD4001 = 2 AND V4071 = 2 THEN SOMA_QT_PESSOAS
                                    WHEN V3002 = 2 AND VD4002 = 2 AND V4071 = 2 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT2_NEM_NEM_NEM,
                                    
                                    #Não estuda, não trabalha e procura emprego#
                                    CASE
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN VD4001 IS NULL THEN NULL
                                    WHEN V3002 = 2 AND VD4001 = 2 AND V4071 = 1 THEN SOMA_QT_PESSOAS
                                    WHEN V3002 = 2 AND VD4002 = 2 AND V4071 = 1 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT2_NEM_NEM_PRO,
                                    
                                    #Denominador para alocação do tempo do jovem#
                                    CASE
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN VD4001 IS NULL THEN NULL
                                    ELSE SOMA_QT_PESSOAS
                                    END AS DENOM_QT2_ALOC_TEMP,
                                    
                                    #Frequenta o Ensino Fundamental#
                                    CASE
                                    WHEN CAT_STATUS_EF IS NULL THEN NULL
                                    WHEN CAT_STATUS_EF = 'B' THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT3_FREQ_EF,
                                    
                                    #Concluiu o Ensino Fundamental#
                                    CASE
                                    WHEN CAT_STATUS_EF IS NULL THEN NULL
                                    WHEN CAT_STATUS_EF = 'C' THEN SOMA_QT_PESSOAS
                                    WHEN CAT_STATUS_EF = 'D' THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT3_CONCLUIU_EF,
                                    
                                    #Concluiu o Ensino Fundamental e Frequenta o ensino médio#
                                    CASE
                                    WHEN CAT_STATUS_EF IS NULL THEN NULL
                                    WHEN CAT_STATUS_EF = 'C' THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT3_CONCLUIU_EF_FREQ_ENSINO_MEDIO,
                                    
                                    #Concluiu o Ensino Fundamental e não Frequenta o ensino médio#
                                    CASE
                                    WHEN CAT_STATUS_EF IS NULL THEN NULL
                                    WHEN CAT_STATUS_EF = 'D' THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT3_CONCLUIU_EF_N_FREQ_ENSINO_MEDIO,
                                    
                                    #Não frequenta escola e não concluiu o Ensino Fundamental#
                                    CASE
                                    WHEN CAT_STATUS_EF IS NULL THEN NULL
                                    WHEN CAT_STATUS_EF = 'A' THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT3_N_FREQ_N_CONCLUIU_EF,
                                    
                                    #Frequenta o Ensino Médio#
                                    CASE
                                    WHEN CAT_STATUS_EM IS NULL THEN NULL
                                    WHEN CAT_STATUS_EM = 'B' THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT3_FREQ_EM,
                                    
                                    #Concluiu o Ensino Médio#
                                    CASE
                                    WHEN CAT_STATUS_EM IS NULL THEN NULL
                                    WHEN CAT_STATUS_EM = 'C' THEN SOMA_QT_PESSOAS
                                    WHEN CAT_STATUS_EM = 'D' THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT3_CONCLUIU_EM,
                                    
                                    #Concluiu o Ensino Médio e frequenta o ensino superior#
                                    CASE
                                    WHEN CAT_STATUS_EM IS NULL THEN NULL
                                    WHEN CAT_STATUS_EM = 'C' THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT3_CONCLUIU_EM_FREQ_ENSINO_SUP,
                                    
                                    #Concluiu o Ensino Médio e não frequenta o ensino superior#
                                    CASE
                                    WHEN CAT_STATUS_EM IS NULL THEN NULL
                                    WHEN CAT_STATUS_EM = 'D' THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT3_CONCLUIU_EM_N_FREQ_ENSINO_SUP,
                                    
                                    #Não frequenta escola e não concluiu o ensino médio#
                                    CASE
                                    WHEN CAT_STATUS_EM IS NULL THEN NULL
                                    WHEN CAT_STATUS_EM = 'A' THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT3_N_FREQ_N_CONCLUIU_EM,
                                    
                                    #Pessoas com informação sobre Nível de instrução e frequencia a escola#
                                    CASE
                                    WHEN VD3004 IS NULL THEN NULL
                                    ELSE SOMA_QT_PESSOAS
                                    END AS DENOM_QT3_STATUS_ESCOLAR,
                                    
                                    #Atraso e matrículas no ensino fundamental, primeiro ano#
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN CAT_IDADE_REF_31MARCO > 6 AND CAT_IDADE_REF_31MARCO <= 150
                                    AND V3002 = 1 AND V3003 = 3 AND V3004 = 2 AND V3006 = 1 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT4_ATRASO_EF1,
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN V3002 = 1 AND V3003 = 3 AND V3004 = 2 AND V3006 = 1 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS DENOM_QT4_MATRICULAS_EF1,
                                    
                                    #Atraso e matrículas no ensino fundamental, segundo ano / primeira série#
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN CAT_IDADE_REF_31MARCO > 7 AND CAT_IDADE_REF_31MARCO <= 150
                                    AND V3002 = 1 AND V3003 = 3 AND V3004 = 1 AND V3006 = 1 THEN SOMA_QT_PESSOAS
                                    WHEN CAT_IDADE_REF_31MARCO > 7 AND CAT_IDADE_REF_31MARCO <= 150
                                    AND V3002 = 1 AND V3003 = 3 AND V3004 = 2 AND V3006 = 2 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT5_ATRASO_EF2,
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN V3002 = 1 AND V3003 = 3 AND V3004 = 1 AND V3006 = 1 THEN SOMA_QT_PESSOAS
                                    WHEN V3002 = 1 AND V3003 = 3 AND V3004 = 2 AND V3006 = 2 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS DENOM_QT5_MATRICULAS_EF2,
                                    
                                    #Atraso e matrículas no ensino fundamental, terceiro ano / segunda série#
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN CAT_IDADE_REF_31MARCO > 8 AND CAT_IDADE_REF_31MARCO <= 150
                                    AND V3002 = 1 AND V3003 = 3 AND V3004 = 1 AND V3006 = 2 THEN SOMA_QT_PESSOAS
                                    WHEN CAT_IDADE_REF_31MARCO > 8 AND CAT_IDADE_REF_31MARCO <= 150
                                    AND V3002 = 1 AND V3003 = 3 AND V3004 = 2 AND V3006 = 3 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT6_ATRASO_EF3,
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN V3002 = 1 AND V3003 = 3 AND V3004 = 1 AND V3006 = 2 THEN SOMA_QT_PESSOAS
                                    WHEN V3002 = 1 AND V3003 = 3 AND V3004 = 2 AND V3006 = 3 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS DENOM_QT6_MATRICULAS_EF3,
                                    
                                    #Atraso e matrículas no ensino fundamental, quarto ano / terceira série#
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN CAT_IDADE_REF_31MARCO > 9 AND CAT_IDADE_REF_31MARCO <= 150
                                    AND V3002 = 1 AND V3003 = 3 AND V3004 = 1 AND V3006 = 3 THEN SOMA_QT_PESSOAS
                                    WHEN CAT_IDADE_REF_31MARCO > 9 AND CAT_IDADE_REF_31MARCO <= 150
                                    AND V3002 = 1 AND V3003 = 3 AND V3004 = 2 AND V3006 = 4 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT7_ATRASO_EF4,
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN V3002 = 1 AND V3003 = 3 AND V3004 = 1 AND V3006 = 3 THEN SOMA_QT_PESSOAS
                                    WHEN V3002 = 1 AND V3003 = 3 AND V3004 = 2 AND V3006 = 4 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS DENOM_QT7_MATRICULAS_EF4,
                                    
                                    #Atraso e matrículas no ensino fundamental, quinto ano / quarta série#
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN CAT_IDADE_REF_31MARCO > 10 AND CAT_IDADE_REF_31MARCO <= 150
                                    AND V3002 = 1 AND V3003 = 3 AND V3004 = 1 AND V3006 = 4 THEN SOMA_QT_PESSOAS
                                    WHEN CAT_IDADE_REF_31MARCO > 10 AND CAT_IDADE_REF_31MARCO <= 150
                                    AND V3002 = 1 AND V3003 = 3 AND V3004 = 2 AND V3006 = 5 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT8_ATRASO_EF5,
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN V3002 = 1 AND V3003 = 3 AND V3004 = 1 AND V3006 = 4 THEN SOMA_QT_PESSOAS
                                    WHEN V3002 = 1 AND V3003 = 3 AND V3004 = 2 AND V3006 = 5 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS DENOM_QT8_MATRICULAS_EF5,
                                    
                                    #Atraso e matrículas no ensino fundamental, sexto ano / quinta série#
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN CAT_IDADE_REF_31MARCO > 11 AND CAT_IDADE_REF_31MARCO <= 150
                                    AND V3002 = 1 AND V3003 = 3 AND V3004 = 1 AND V3006 = 5 THEN SOMA_QT_PESSOAS
                                    WHEN CAT_IDADE_REF_31MARCO > 11 AND CAT_IDADE_REF_31MARCO <= 150
                                    AND V3002 = 1 AND V3003 = 3 AND V3004 = 2 AND V3006 = 6 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT9_ATRASO_EF6,
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN V3002 = 1 AND V3003 = 3 AND V3004 = 1 AND V3006 = 5 THEN SOMA_QT_PESSOAS
                                    WHEN V3002 = 1 AND V3003 = 3 AND V3004 = 2 AND V3006 = 6 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS DENOM_QT9_MATRICULAS_EF6,
                                    
                                    #Atraso e matrículas no ensino fundamental, sétimo ano / sexta série#
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN CAT_IDADE_REF_31MARCO > 12 AND CAT_IDADE_REF_31MARCO <= 150
                                    AND V3002 = 1 AND V3003 = 3 AND V3004 = 1 AND V3006 = 6 THEN SOMA_QT_PESSOAS
                                    WHEN CAT_IDADE_REF_31MARCO > 12 AND CAT_IDADE_REF_31MARCO <= 150
                                    AND V3002 = 1 AND V3003 = 3 AND V3004 = 2 AND V3006 = 7 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT10_ATRASO_EF7,
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN V3002 = 1 AND V3003 = 3 AND V3004 = 1 AND V3006 = 6 THEN SOMA_QT_PESSOAS
                                    WHEN V3002 = 1 AND V3003 = 3 AND V3004 = 2 AND V3006 = 7 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS DENOM_QT10_MATRICULAS_EF7,
                                    
                                    #Atraso e matrículas no ensino fundamental, oitavo ano / sétima série#
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN CAT_IDADE_REF_31MARCO > 13 AND CAT_IDADE_REF_31MARCO <= 150
                                    AND V3002 = 1 AND V3003 = 3 AND V3004 = 1 AND V3006 = 7 THEN SOMA_QT_PESSOAS
                                    WHEN CAT_IDADE_REF_31MARCO > 13 AND CAT_IDADE_REF_31MARCO <= 150
                                    AND V3002 = 1 AND V3003 = 3 AND V3004 = 2 AND V3006 = 8 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT11_ATRASO_EF8,
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN V3002 = 1 AND V3003 = 3 AND V3004 = 1 AND V3006 = 7 THEN SOMA_QT_PESSOAS
                                    WHEN V3002 = 1 AND V3003 = 3 AND V3004 = 2 AND V3006 = 8 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS DENOM_QT11_MATRICULAS_EF8,
                                    
                                    #Atraso e matrículas no ensino fundamental, nono ano / oitava série#
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN CAT_IDADE_REF_31MARCO > 14 AND CAT_IDADE_REF_31MARCO <= 150
                                    AND V3002 = 1 AND V3003 = 3 AND V3004 = 1 AND V3006 = 8 THEN SOMA_QT_PESSOAS
                                    WHEN CAT_IDADE_REF_31MARCO > 14 AND CAT_IDADE_REF_31MARCO <= 150
                                    AND V3002 = 1 AND V3003 = 3 AND V3004 = 2 AND V3006 = 9 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT12_ATRASO_EF9,
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN V3002 = 1 AND V3003 = 3 AND V3004 = 1 AND V3006 = 8 THEN SOMA_QT_PESSOAS
                                    WHEN V3002 = 1 AND V3003 = 3 AND V3004 = 2 AND V3006 = 9 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS DENOM_QT12_MATRICULAS_EF9,
                                    
                                    #Atraso e matrículas no ensino médio, primeiro ano#
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN CAT_IDADE_REF_31MARCO > 15 AND CAT_IDADE_REF_31MARCO <= 150
                                    AND V3002 = 1 AND V3003 = 5 AND V3006 = 1 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT13_ATRASO_EM1,
                                    CASE
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN V3002 = 1 AND V3003 = 5 AND V3006 = 1 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS DENOM_QT13_MATRICULAS_EM1,
                                    
                                    #Atraso e matrículas no ensino médio, segundo ano#
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN CAT_IDADE_REF_31MARCO > 16 AND CAT_IDADE_REF_31MARCO <= 150
                                    AND V3002 = 1 AND V3003 = 5 AND V3006 = 2 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT14_ATRASO_EM2,
                                    CASE
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN V3002 = 1 AND V3003 = 5 AND V3006 = 2 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS DENOM_QT14_MATRICULAS_EM2,
                                    
                                    #Atraso e matrículas no ensino médio, terceiro ano#
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN CAT_IDADE_REF_31MARCO > 17 AND CAT_IDADE_REF_31MARCO <= 150
                                    AND V3002 = 1 AND V3003 = 5 AND V3006 = 3 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT15_ATRASO_EM3,
                                    CASE
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN V3002 = 1 AND V3003 = 5 AND V3006 = 3 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS DENOM_QT15_MATRICULAS_EM3,
                                    
                                    #Atraso e matrículas no ensino médio, quarto ano#
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN CAT_IDADE_REF_31MARCO > 18 AND CAT_IDADE_REF_31MARCO <= 150
                                    AND V3002 = 1 AND V3003 = 5 AND V3006 = 4 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT16_ATRASO_EM4,
                                    CASE
                                    WHEN CAT_IDADE_REF_31MARCO IS NULL THEN NULL
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3003 IS NULL THEN NULL
                                    WHEN V3006 IS NULL THEN NULL
                                    WHEN CAT_IDADE_REF_31MARCO > 18 AND CAT_IDADE_REF_31MARCO <= 150
                                    AND V3002 = 1 AND V3003 = 5 AND V3006 = 4 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS DENOM_QT16_MATRICULAS_EM4,
                                    
                                    #Frequenta escola#
                                    CASE
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3002 = 1 THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS NUMER_QT9_FREQ_ESC,
                                    CASE
                                    WHEN V3002 IS NULL THEN NULL
                                    WHEN V3002 IN (1,2) THEN SOMA_QT_PESSOAS
                                    ELSE 0.0
                                    END AS DENOM_QT9
                                    FROM pesTEMP
                                    "))
  
  setDT(pesTEMP)
  
  ############################################
  # TRANSFORMAR OS CAMPOS NOVOS EM NUMERICOS #
  ############################################
  
  pesTEMP[, `:=` (
    
    NUMER_QT1_N_SABE_LER  = as.numeric(NUMER_QT1_N_SABE_LER),
    DENOM_QT1 = as.numeric(DENOM_QT1),
    NUMER_QT2_TRAB_EST  = as.numeric(NUMER_QT2_TRAB_EST),
    NUMER_QT2_SO_TRAB = as.numeric(NUMER_QT2_SO_TRAB),
    NUMER_QT2_SO_ESTUDA  = as.numeric(NUMER_QT2_SO_ESTUDA),
    NUMER_QT2_NEM_NEM = as.numeric(NUMER_QT2_NEM_NEM),
    NUMER_QT2_NEM_NEM_NEM  = as.numeric(NUMER_QT2_NEM_NEM_NEM),
    NUMER_QT2_NEM_NEM_PRO = as.numeric(NUMER_QT2_NEM_NEM_PRO),
    DENOM_QT2_ALOC_TEMP  = as.numeric(DENOM_QT2_ALOC_TEMP),
    NUMER_QT3_FREQ_EF = as.numeric(NUMER_QT3_FREQ_EF),
    NUMER_QT3_CONCLUIU_EF  = as.numeric(NUMER_QT3_CONCLUIU_EF),
    NUMER_QT3_CONCLUIU_EF_FREQ_ENSINO_MEDIO = as.numeric(NUMER_QT3_CONCLUIU_EF_FREQ_ENSINO_MEDIO),
    NUMER_QT3_CONCLUIU_EF_N_FREQ_ENSINO_MEDIO = as.numeric(NUMER_QT3_CONCLUIU_EF_N_FREQ_ENSINO_MEDIO),
    NUMER_QT3_N_FREQ_N_CONCLUIU_EF = as.numeric(NUMER_QT3_N_FREQ_N_CONCLUIU_EF),
    NUMER_QT3_FREQ_EM  = as.numeric(NUMER_QT3_FREQ_EM),
    NUMER_QT3_CONCLUIU_EM = as.numeric(NUMER_QT3_CONCLUIU_EM),
    NUMER_QT3_CONCLUIU_EM_FREQ_ENSINO_SUP  = as.numeric(NUMER_QT3_CONCLUIU_EM_FREQ_ENSINO_SUP),
    NUMER_QT3_CONCLUIU_EM_N_FREQ_ENSINO_SUP = as.numeric(NUMER_QT3_CONCLUIU_EM_N_FREQ_ENSINO_SUP),
    NUMER_QT3_N_FREQ_N_CONCLUIU_EM  = as.numeric(NUMER_QT3_N_FREQ_N_CONCLUIU_EM),
    DENOM_QT3_STATUS_ESCOLAR = as.numeric(DENOM_QT3_STATUS_ESCOLAR),
    NUMER_QT4_ATRASO_EF1  = as.numeric(NUMER_QT4_ATRASO_EF1),
    DENOM_QT4_MATRICULAS_EF1 = as.numeric(DENOM_QT4_MATRICULAS_EF1),
    NUMER_QT5_ATRASO_EF2  = as.numeric(NUMER_QT5_ATRASO_EF2),
    DENOM_QT5_MATRICULAS_EF2 = as.numeric(DENOM_QT5_MATRICULAS_EF2),
    NUMER_QT6_ATRASO_EF3  = as.numeric(NUMER_QT6_ATRASO_EF3),
    DENOM_QT6_MATRICULAS_EF3 = as.numeric(DENOM_QT6_MATRICULAS_EF3),
    NUMER_QT7_ATRASO_EF4  = as.numeric(NUMER_QT7_ATRASO_EF4),
    DENOM_QT7_MATRICULAS_EF4 = as.numeric(DENOM_QT7_MATRICULAS_EF4),
    NUMER_QT8_ATRASO_EF5  = as.numeric(NUMER_QT8_ATRASO_EF5),
    DENOM_QT8_MATRICULAS_EF5 = as.numeric(DENOM_QT8_MATRICULAS_EF5),
    NUMER_QT9_ATRASO_EF6  = as.numeric(NUMER_QT9_ATRASO_EF6),
    DENOM_QT9_MATRICULAS_EF6 = as.numeric(DENOM_QT9_MATRICULAS_EF6),
    NUMER_QT10_ATRASO_EF7  = as.numeric(NUMER_QT10_ATRASO_EF7),
    DENOM_QT10_MATRICULAS_EF7 = as.numeric(DENOM_QT10_MATRICULAS_EF7),
    NUMER_QT11_ATRASO_EF8  = as.numeric(NUMER_QT11_ATRASO_EF8),
    DENOM_QT11_MATRICULAS_EF8 = as.numeric(DENOM_QT11_MATRICULAS_EF8),
    NUMER_QT12_ATRASO_EF9  = as.numeric(NUMER_QT12_ATRASO_EF9),
    DENOM_QT12_MATRICULAS_EF9 = as.numeric(DENOM_QT12_MATRICULAS_EF9),
    NUMER_QT13_ATRASO_EM1  = as.numeric(NUMER_QT13_ATRASO_EM1),
    DENOM_QT13_MATRICULAS_EM1 = as.numeric(DENOM_QT13_MATRICULAS_EM1),
    NUMER_QT14_ATRASO_EM2  = as.numeric(NUMER_QT14_ATRASO_EM2),
    DENOM_QT14_MATRICULAS_EM2 = as.numeric(DENOM_QT14_MATRICULAS_EM2),
    NUMER_QT15_ATRASO_EM3  = as.numeric(NUMER_QT15_ATRASO_EM3),
    DENOM_QT15_MATRICULAS_EM3 = as.numeric(DENOM_QT15_MATRICULAS_EM3),
    NUMER_QT16_ATRASO_EM4  = as.numeric(NUMER_QT16_ATRASO_EM4),
    DENOM_QT16_MATRICULAS_EM4 = as.numeric(DENOM_QT16_MATRICULAS_EM4),
    NUMER_QT9_FREQ_ESC  = as.numeric(NUMER_QT9_FREQ_ESC),
    DENOM_QT9  = as.numeric(DENOM_QT9)
  )]
  
  
  # AGREGAÇÃO DOS REGISTROS -------------------------------------------------
  
  # CRIAR CHAVE PARA QUE O ARQUIVO ESTEJA ORDENADO ANTES DE SER AGRUPADO
  
  setkey(pesTEMP,
         Ano,
         Trimestre,
         UF,
         CAT_NIV_GEO,
         CAT_COR_RACA,
         V2007,
         CAT_FX_ETARIA,
         CAT_FX_ETARIA_JUV,
         CAT_FX_ETARIA_REF_31MARCO,
         CAT_FX_ETARIA_REF_31MARCO_NO_INPUT,
         CAT_TIPO_EDUCA,
         CAT_MOTIVOS,
         CAT_REM_HAB_QUINTIL_UF,
         CAT_REM_HAB_QUINTIL_REGIAO,
         CAT_REM_HAB_QUINTIL_BRASIL  
  )
  
  ###################################### AGRUPAR POR CAPITAL, RESTO DA RM E INTERIOR
  
  pesTEMP_CP_RM_IN = pesTEMP[,
                             list(
                               SOMA_AMOSTRAS=sum(SOMA_AMOSTRAS, na.rm = TRUE),
                               SOMA_QT_PESSOAS=sum(SOMA_QT_PESSOAS, na.rm = TRUE),
                               NUMER_QT1_N_SABE_LER =sum(NUMER_QT1_N_SABE_LER, na.rm = TRUE),
                               DENOM_QT1=sum(DENOM_QT1, na.rm = TRUE),
                               NUMER_QT2_TRAB_EST =sum(NUMER_QT2_TRAB_EST, na.rm = TRUE),
                               NUMER_QT2_SO_TRAB=sum(NUMER_QT2_SO_TRAB, na.rm = TRUE),
                               NUMER_QT2_SO_ESTUDA =sum(NUMER_QT2_SO_ESTUDA, na.rm = TRUE),
                               NUMER_QT2_NEM_NEM=sum(NUMER_QT2_NEM_NEM, na.rm = TRUE),
                               NUMER_QT2_NEM_NEM_NEM =sum(NUMER_QT2_NEM_NEM_NEM, na.rm = TRUE),
                               NUMER_QT2_NEM_NEM_PRO=sum(NUMER_QT2_NEM_NEM_PRO, na.rm = TRUE),
                               DENOM_QT2_ALOC_TEMP =sum(DENOM_QT2_ALOC_TEMP, na.rm = TRUE),
                               NUMER_QT3_FREQ_EF=sum(NUMER_QT3_FREQ_EF, na.rm = TRUE),
                               NUMER_QT3_CONCLUIU_EF =sum(NUMER_QT3_CONCLUIU_EF, na.rm = TRUE),
                               NUMER_QT3_CONCLUIU_EF_FREQ_ENSINO_MEDIO=sum(NUMER_QT3_CONCLUIU_EF_FREQ_ENSINO_MEDIO, na.rm = TRUE),
                               NUMER_QT3_CONCLUIU_EF_N_FREQ_ENSINO_MEDIO=sum(NUMER_QT3_CONCLUIU_EF_N_FREQ_ENSINO_MEDIO, na.rm = TRUE),
                               NUMER_QT3_N_FREQ_N_CONCLUIU_EF=sum(NUMER_QT3_N_FREQ_N_CONCLUIU_EF, na.rm = TRUE),
                               NUMER_QT3_FREQ_EM =sum(NUMER_QT3_FREQ_EM, na.rm = TRUE),
                               NUMER_QT3_CONCLUIU_EM=sum(NUMER_QT3_CONCLUIU_EM, na.rm = TRUE),
                               NUMER_QT3_CONCLUIU_EM_FREQ_ENSINO_SUP =sum(NUMER_QT3_CONCLUIU_EM_FREQ_ENSINO_SUP, na.rm = TRUE),
                               NUMER_QT3_CONCLUIU_EM_N_FREQ_ENSINO_SUP=sum(NUMER_QT3_CONCLUIU_EM_N_FREQ_ENSINO_SUP, na.rm = TRUE),
                               NUMER_QT3_N_FREQ_N_CONCLUIU_EM =sum(NUMER_QT3_N_FREQ_N_CONCLUIU_EM, na.rm = TRUE),
                               DENOM_QT3_STATUS_ESCOLAR=sum(DENOM_QT3_STATUS_ESCOLAR, na.rm = TRUE),
                               NUMER_QT4_ATRASO_EF1 =sum(NUMER_QT4_ATRASO_EF1, na.rm = TRUE),
                               DENOM_QT4_MATRICULAS_EF1=sum(DENOM_QT4_MATRICULAS_EF1, na.rm = TRUE),
                               NUMER_QT5_ATRASO_EF2 =sum(NUMER_QT5_ATRASO_EF2, na.rm = TRUE),
                               DENOM_QT5_MATRICULAS_EF2=sum(DENOM_QT5_MATRICULAS_EF2, na.rm = TRUE),
                               NUMER_QT6_ATRASO_EF3 =sum(NUMER_QT6_ATRASO_EF3, na.rm = TRUE),
                               DENOM_QT6_MATRICULAS_EF3=sum(DENOM_QT6_MATRICULAS_EF3, na.rm = TRUE),
                               NUMER_QT7_ATRASO_EF4 =sum(NUMER_QT7_ATRASO_EF4, na.rm = TRUE),
                               DENOM_QT7_MATRICULAS_EF4=sum(DENOM_QT7_MATRICULAS_EF4, na.rm = TRUE),
                               NUMER_QT8_ATRASO_EF5 =sum(NUMER_QT8_ATRASO_EF5, na.rm = TRUE),
                               DENOM_QT8_MATRICULAS_EF5=sum(DENOM_QT8_MATRICULAS_EF5, na.rm = TRUE),
                               NUMER_QT9_ATRASO_EF6 =sum(NUMER_QT9_ATRASO_EF6, na.rm = TRUE),
                               DENOM_QT9_MATRICULAS_EF6=sum(DENOM_QT9_MATRICULAS_EF6, na.rm = TRUE),
                               NUMER_QT10_ATRASO_EF7 =sum(NUMER_QT10_ATRASO_EF7, na.rm = TRUE),
                               DENOM_QT10_MATRICULAS_EF7=sum(DENOM_QT10_MATRICULAS_EF7, na.rm = TRUE),
                               NUMER_QT11_ATRASO_EF8 =sum(NUMER_QT11_ATRASO_EF8, na.rm = TRUE),
                               DENOM_QT11_MATRICULAS_EF8=sum(DENOM_QT11_MATRICULAS_EF8, na.rm = TRUE),
                               NUMER_QT12_ATRASO_EF9 =sum(NUMER_QT12_ATRASO_EF9, na.rm = TRUE),
                               DENOM_QT12_MATRICULAS_EF9=sum(DENOM_QT12_MATRICULAS_EF9, na.rm = TRUE),
                               NUMER_QT13_ATRASO_EM1 =sum(NUMER_QT13_ATRASO_EM1, na.rm = TRUE),
                               DENOM_QT13_MATRICULAS_EM1=sum(DENOM_QT13_MATRICULAS_EM1, na.rm = TRUE),
                               NUMER_QT14_ATRASO_EM2 =sum(NUMER_QT14_ATRASO_EM2, na.rm = TRUE),
                               DENOM_QT14_MATRICULAS_EM2=sum(DENOM_QT14_MATRICULAS_EM2, na.rm = TRUE),
                               NUMER_QT15_ATRASO_EM3 =sum(NUMER_QT15_ATRASO_EM3, na.rm = TRUE),
                               DENOM_QT15_MATRICULAS_EM3=sum(DENOM_QT15_MATRICULAS_EM3, na.rm = TRUE),
                               NUMER_QT16_ATRASO_EM4 =sum(NUMER_QT16_ATRASO_EM4 , na.rm = TRUE),
                               DENOM_QT16_MATRICULAS_EM4=sum(DENOM_QT16_MATRICULAS_EM4, na.rm = TRUE),
                               NUMER_QT9_FREQ_ESC =sum(NUMER_QT9_FREQ_ESC, na.rm = TRUE),
                               DENOM_QT9 =sum(DENOM_QT9, na.rm = TRUE)
                             ),
                             by=list(
                               CAT_ANO=Ano,
                               CAT_TRIMESTRE=Trimestre,
                               CAT_LOCAL=CAT_NIV_GEO,
                               CAT_SEXO=V2007,
                               CAT_COR_RACA,
                               CAT_FX_ETARIA,
                               CAT_FX_ETARIA_JUV,
                               CAT_FX_ETARIA_REF_31MARCO,
                               CAT_FX_ETARIA_REF_31MARCO_NO_INPUT,
                               CAT_TIPO_EDUCA,
                               CAT_MOTIVOS,
                               CAT_RDTPC_QUINTIL = CAT_REM_HAB_QUINTIL_UF
                             )    
                             ]
  
  ###################################### AGRUPAR POR RM E INTERIOR
  
  pesTEMP_RM_IN = pesTEMP[CAT_NIV_GEO %% 10 != 3][,# Somente com R.M, ver como foi criada a NIV_GEO
                                                  list(
                                                    SOMA_AMOSTRAS=sum(SOMA_AMOSTRAS, na.rm = TRUE),
                                                    SOMA_QT_PESSOAS=sum(SOMA_QT_PESSOAS, na.rm = TRUE),
                                                    NUMER_QT1_N_SABE_LER =sum(NUMER_QT1_N_SABE_LER, na.rm = TRUE),
                                                    DENOM_QT1=sum(DENOM_QT1, na.rm = TRUE),
                                                    NUMER_QT2_TRAB_EST =sum(NUMER_QT2_TRAB_EST, na.rm = TRUE),
                                                    NUMER_QT2_SO_TRAB=sum(NUMER_QT2_SO_TRAB, na.rm = TRUE),
                                                    NUMER_QT2_SO_ESTUDA =sum(NUMER_QT2_SO_ESTUDA, na.rm = TRUE),
                                                    NUMER_QT2_NEM_NEM=sum(NUMER_QT2_NEM_NEM, na.rm = TRUE),
                                                    NUMER_QT2_NEM_NEM_NEM =sum(NUMER_QT2_NEM_NEM_NEM, na.rm = TRUE),
                                                    NUMER_QT2_NEM_NEM_PRO=sum(NUMER_QT2_NEM_NEM_PRO, na.rm = TRUE),
                                                    DENOM_QT2_ALOC_TEMP =sum(DENOM_QT2_ALOC_TEMP, na.rm = TRUE),
                                                    NUMER_QT3_FREQ_EF=sum(NUMER_QT3_FREQ_EF, na.rm = TRUE),
                                                    NUMER_QT3_CONCLUIU_EF =sum(NUMER_QT3_CONCLUIU_EF, na.rm = TRUE),
                                                    NUMER_QT3_CONCLUIU_EF_FREQ_ENSINO_MEDIO=sum(NUMER_QT3_CONCLUIU_EF_FREQ_ENSINO_MEDIO, na.rm = TRUE),
                                                    NUMER_QT3_CONCLUIU_EF_N_FREQ_ENSINO_MEDIO=sum(NUMER_QT3_CONCLUIU_EF_N_FREQ_ENSINO_MEDIO, na.rm = TRUE),
                                                    NUMER_QT3_N_FREQ_N_CONCLUIU_EF=sum(NUMER_QT3_N_FREQ_N_CONCLUIU_EF, na.rm = TRUE),
                                                    NUMER_QT3_FREQ_EM =sum(NUMER_QT3_FREQ_EM, na.rm = TRUE),
                                                    NUMER_QT3_CONCLUIU_EM=sum(NUMER_QT3_CONCLUIU_EM, na.rm = TRUE),
                                                    NUMER_QT3_CONCLUIU_EM_FREQ_ENSINO_SUP =sum(NUMER_QT3_CONCLUIU_EM_FREQ_ENSINO_SUP, na.rm = TRUE),
                                                    NUMER_QT3_CONCLUIU_EM_N_FREQ_ENSINO_SUP=sum(NUMER_QT3_CONCLUIU_EM_N_FREQ_ENSINO_SUP, na.rm = TRUE),
                                                    NUMER_QT3_N_FREQ_N_CONCLUIU_EM =sum(NUMER_QT3_N_FREQ_N_CONCLUIU_EM, na.rm = TRUE),
                                                    DENOM_QT3_STATUS_ESCOLAR=sum(DENOM_QT3_STATUS_ESCOLAR, na.rm = TRUE),
                                                    NUMER_QT4_ATRASO_EF1 =sum(NUMER_QT4_ATRASO_EF1, na.rm = TRUE),
                                                    DENOM_QT4_MATRICULAS_EF1=sum(DENOM_QT4_MATRICULAS_EF1, na.rm = TRUE),
                                                    NUMER_QT5_ATRASO_EF2 =sum(NUMER_QT5_ATRASO_EF2, na.rm = TRUE),
                                                    DENOM_QT5_MATRICULAS_EF2=sum(DENOM_QT5_MATRICULAS_EF2, na.rm = TRUE),
                                                    NUMER_QT6_ATRASO_EF3 =sum(NUMER_QT6_ATRASO_EF3, na.rm = TRUE),
                                                    DENOM_QT6_MATRICULAS_EF3=sum(DENOM_QT6_MATRICULAS_EF3, na.rm = TRUE),
                                                    NUMER_QT7_ATRASO_EF4 =sum(NUMER_QT7_ATRASO_EF4, na.rm = TRUE),
                                                    DENOM_QT7_MATRICULAS_EF4=sum(DENOM_QT7_MATRICULAS_EF4, na.rm = TRUE),
                                                    NUMER_QT8_ATRASO_EF5 =sum(NUMER_QT8_ATRASO_EF5, na.rm = TRUE),
                                                    DENOM_QT8_MATRICULAS_EF5=sum(DENOM_QT8_MATRICULAS_EF5, na.rm = TRUE),
                                                    NUMER_QT9_ATRASO_EF6 =sum(NUMER_QT9_ATRASO_EF6, na.rm = TRUE),
                                                    DENOM_QT9_MATRICULAS_EF6=sum(DENOM_QT9_MATRICULAS_EF6, na.rm = TRUE),
                                                    NUMER_QT10_ATRASO_EF7 =sum(NUMER_QT10_ATRASO_EF7, na.rm = TRUE),
                                                    DENOM_QT10_MATRICULAS_EF7=sum(DENOM_QT10_MATRICULAS_EF7, na.rm = TRUE),
                                                    NUMER_QT11_ATRASO_EF8 =sum(NUMER_QT11_ATRASO_EF8, na.rm = TRUE),
                                                    DENOM_QT11_MATRICULAS_EF8=sum(DENOM_QT11_MATRICULAS_EF8, na.rm = TRUE),
                                                    NUMER_QT12_ATRASO_EF9 =sum(NUMER_QT12_ATRASO_EF9, na.rm = TRUE),
                                                    DENOM_QT12_MATRICULAS_EF9=sum(DENOM_QT12_MATRICULAS_EF9, na.rm = TRUE),
                                                    NUMER_QT13_ATRASO_EM1 =sum(NUMER_QT13_ATRASO_EM1, na.rm = TRUE),
                                                    DENOM_QT13_MATRICULAS_EM1=sum(DENOM_QT13_MATRICULAS_EM1, na.rm = TRUE),
                                                    NUMER_QT14_ATRASO_EM2 =sum(NUMER_QT14_ATRASO_EM2, na.rm = TRUE),
                                                    DENOM_QT14_MATRICULAS_EM2=sum(DENOM_QT14_MATRICULAS_EM2, na.rm = TRUE),
                                                    NUMER_QT15_ATRASO_EM3 =sum(NUMER_QT15_ATRASO_EM3, na.rm = TRUE),
                                                    DENOM_QT15_MATRICULAS_EM3=sum(DENOM_QT15_MATRICULAS_EM3, na.rm = TRUE),
                                                    NUMER_QT16_ATRASO_EM4 =sum(NUMER_QT16_ATRASO_EM4 , na.rm = TRUE),
                                                    DENOM_QT16_MATRICULAS_EM4=sum(DENOM_QT16_MATRICULAS_EM4, na.rm = TRUE),
                                                    NUMER_QT9_FREQ_ESC =sum(NUMER_QT9_FREQ_ESC, na.rm = TRUE),
                                                    DENOM_QT9 =sum(DENOM_QT9, na.rm = TRUE)
                                                  ),
                                                  by=list(
                                                    CAT_ANO=Ano,
                                                    CAT_TRIMESTRE=Trimestre,
                                                    CAT_LOCAL=floor(CAT_NIV_GEO/10)*10 + 2,
                                                    CAT_SEXO=V2007,
                                                    CAT_COR_RACA,
                                                    CAT_FX_ETARIA,
                                                    CAT_FX_ETARIA_JUV,
                                                    CAT_FX_ETARIA_REF_31MARCO,
                                                    CAT_FX_ETARIA_REF_31MARCO_NO_INPUT,
                                                    CAT_TIPO_EDUCA,
                                                    CAT_MOTIVOS,
                                                    CAT_RDTPC_QUINTIL = CAT_REM_HAB_QUINTIL_UF
                                                  )#Este vetor abaixo é para manter somente os registros com região metropolitana!    
                                                  ][CAT_LOCAL %in% c(132,152,162,212,232,242,252,262,272,282,292,312,322,332,352,412,422,432,512,522)]
  
  ###################################### AGRUPAR POR UF
  
  pesTEMPUF = pesTEMP[,
                      list(
                        SOMA_AMOSTRAS=sum(SOMA_AMOSTRAS, na.rm = TRUE),
                        SOMA_QT_PESSOAS=sum(SOMA_QT_PESSOAS, na.rm = TRUE),
                        NUMER_QT1_N_SABE_LER =sum(NUMER_QT1_N_SABE_LER, na.rm = TRUE),
                        DENOM_QT1=sum(DENOM_QT1, na.rm = TRUE),
                        NUMER_QT2_TRAB_EST =sum(NUMER_QT2_TRAB_EST, na.rm = TRUE),
                        NUMER_QT2_SO_TRAB=sum(NUMER_QT2_SO_TRAB, na.rm = TRUE),
                        NUMER_QT2_SO_ESTUDA =sum(NUMER_QT2_SO_ESTUDA, na.rm = TRUE),
                        NUMER_QT2_NEM_NEM=sum(NUMER_QT2_NEM_NEM, na.rm = TRUE),
                        NUMER_QT2_NEM_NEM_NEM =sum(NUMER_QT2_NEM_NEM_NEM, na.rm = TRUE),
                        NUMER_QT2_NEM_NEM_PRO=sum(NUMER_QT2_NEM_NEM_PRO, na.rm = TRUE),
                        DENOM_QT2_ALOC_TEMP =sum(DENOM_QT2_ALOC_TEMP, na.rm = TRUE),
                        NUMER_QT3_FREQ_EF=sum(NUMER_QT3_FREQ_EF, na.rm = TRUE),
                        NUMER_QT3_CONCLUIU_EF =sum(NUMER_QT3_CONCLUIU_EF, na.rm = TRUE),
                        NUMER_QT3_CONCLUIU_EF_FREQ_ENSINO_MEDIO=sum(NUMER_QT3_CONCLUIU_EF_FREQ_ENSINO_MEDIO, na.rm = TRUE),
                        NUMER_QT3_CONCLUIU_EF_N_FREQ_ENSINO_MEDIO=sum(NUMER_QT3_CONCLUIU_EF_N_FREQ_ENSINO_MEDIO, na.rm = TRUE),
                        NUMER_QT3_N_FREQ_N_CONCLUIU_EF=sum(NUMER_QT3_N_FREQ_N_CONCLUIU_EF, na.rm = TRUE),
                        NUMER_QT3_FREQ_EM =sum(NUMER_QT3_FREQ_EM, na.rm = TRUE),
                        NUMER_QT3_CONCLUIU_EM=sum(NUMER_QT3_CONCLUIU_EM, na.rm = TRUE),
                        NUMER_QT3_CONCLUIU_EM_FREQ_ENSINO_SUP =sum(NUMER_QT3_CONCLUIU_EM_FREQ_ENSINO_SUP, na.rm = TRUE),
                        NUMER_QT3_CONCLUIU_EM_N_FREQ_ENSINO_SUP=sum(NUMER_QT3_CONCLUIU_EM_N_FREQ_ENSINO_SUP, na.rm = TRUE),
                        NUMER_QT3_N_FREQ_N_CONCLUIU_EM =sum(NUMER_QT3_N_FREQ_N_CONCLUIU_EM, na.rm = TRUE),
                        DENOM_QT3_STATUS_ESCOLAR=sum(DENOM_QT3_STATUS_ESCOLAR, na.rm = TRUE),
                        NUMER_QT4_ATRASO_EF1 =sum(NUMER_QT4_ATRASO_EF1, na.rm = TRUE),
                        DENOM_QT4_MATRICULAS_EF1=sum(DENOM_QT4_MATRICULAS_EF1, na.rm = TRUE),
                        NUMER_QT5_ATRASO_EF2 =sum(NUMER_QT5_ATRASO_EF2, na.rm = TRUE),
                        DENOM_QT5_MATRICULAS_EF2=sum(DENOM_QT5_MATRICULAS_EF2, na.rm = TRUE),
                        NUMER_QT6_ATRASO_EF3 =sum(NUMER_QT6_ATRASO_EF3, na.rm = TRUE),
                        DENOM_QT6_MATRICULAS_EF3=sum(DENOM_QT6_MATRICULAS_EF3, na.rm = TRUE),
                        NUMER_QT7_ATRASO_EF4 =sum(NUMER_QT7_ATRASO_EF4, na.rm = TRUE),
                        DENOM_QT7_MATRICULAS_EF4=sum(DENOM_QT7_MATRICULAS_EF4, na.rm = TRUE),
                        NUMER_QT8_ATRASO_EF5 =sum(NUMER_QT8_ATRASO_EF5, na.rm = TRUE),
                        DENOM_QT8_MATRICULAS_EF5=sum(DENOM_QT8_MATRICULAS_EF5, na.rm = TRUE),
                        NUMER_QT9_ATRASO_EF6 =sum(NUMER_QT9_ATRASO_EF6, na.rm = TRUE),
                        DENOM_QT9_MATRICULAS_EF6=sum(DENOM_QT9_MATRICULAS_EF6, na.rm = TRUE),
                        NUMER_QT10_ATRASO_EF7 =sum(NUMER_QT10_ATRASO_EF7, na.rm = TRUE),
                        DENOM_QT10_MATRICULAS_EF7=sum(DENOM_QT10_MATRICULAS_EF7, na.rm = TRUE),
                        NUMER_QT11_ATRASO_EF8 =sum(NUMER_QT11_ATRASO_EF8, na.rm = TRUE),
                        DENOM_QT11_MATRICULAS_EF8=sum(DENOM_QT11_MATRICULAS_EF8, na.rm = TRUE),
                        NUMER_QT12_ATRASO_EF9 =sum(NUMER_QT12_ATRASO_EF9, na.rm = TRUE),
                        DENOM_QT12_MATRICULAS_EF9=sum(DENOM_QT12_MATRICULAS_EF9, na.rm = TRUE),
                        NUMER_QT13_ATRASO_EM1 =sum(NUMER_QT13_ATRASO_EM1, na.rm = TRUE),
                        DENOM_QT13_MATRICULAS_EM1=sum(DENOM_QT13_MATRICULAS_EM1, na.rm = TRUE),
                        NUMER_QT14_ATRASO_EM2 =sum(NUMER_QT14_ATRASO_EM2, na.rm = TRUE),
                        DENOM_QT14_MATRICULAS_EM2=sum(DENOM_QT14_MATRICULAS_EM2, na.rm = TRUE),
                        NUMER_QT15_ATRASO_EM3 =sum(NUMER_QT15_ATRASO_EM3, na.rm = TRUE),
                        DENOM_QT15_MATRICULAS_EM3=sum(DENOM_QT15_MATRICULAS_EM3, na.rm = TRUE),
                        NUMER_QT16_ATRASO_EM4 =sum(NUMER_QT16_ATRASO_EM4 , na.rm = TRUE),
                        DENOM_QT16_MATRICULAS_EM4=sum(DENOM_QT16_MATRICULAS_EM4, na.rm = TRUE),
                        NUMER_QT9_FREQ_ESC =sum(NUMER_QT9_FREQ_ESC, na.rm = TRUE),
                        DENOM_QT9 =sum(DENOM_QT9, na.rm = TRUE)
                      ),
                      by=list(
                        CAT_ANO=Ano,
                        CAT_TRIMESTRE=Trimestre,
                        CAT_LOCAL=UF,
                        CAT_SEXO=V2007,
                        CAT_COR_RACA,
                        CAT_FX_ETARIA,
                        CAT_FX_ETARIA_JUV,
                        CAT_FX_ETARIA_REF_31MARCO,
                        CAT_FX_ETARIA_REF_31MARCO_NO_INPUT,
                        CAT_TIPO_EDUCA,
                        CAT_MOTIVOS,
                        CAT_RDTPC_QUINTIL = CAT_REM_HAB_QUINTIL_UF
                      )    
                      ]
  
  
  ###################################### AGRUPAR POR REGIAO
  
  pesTEMPREGIAO = pesTEMP[,
                          list(      
                            SOMA_AMOSTRAS=sum(SOMA_AMOSTRAS, na.rm = TRUE),
                            SOMA_QT_PESSOAS=sum(SOMA_QT_PESSOAS, na.rm = TRUE),
                            NUMER_QT1_N_SABE_LER =sum(NUMER_QT1_N_SABE_LER, na.rm = TRUE),
                            DENOM_QT1=sum(DENOM_QT1, na.rm = TRUE),
                            NUMER_QT2_TRAB_EST =sum(NUMER_QT2_TRAB_EST, na.rm = TRUE),
                            NUMER_QT2_SO_TRAB=sum(NUMER_QT2_SO_TRAB, na.rm = TRUE),
                            NUMER_QT2_SO_ESTUDA =sum(NUMER_QT2_SO_ESTUDA, na.rm = TRUE),
                            NUMER_QT2_NEM_NEM=sum(NUMER_QT2_NEM_NEM, na.rm = TRUE),
                            NUMER_QT2_NEM_NEM_NEM =sum(NUMER_QT2_NEM_NEM_NEM, na.rm = TRUE),
                            NUMER_QT2_NEM_NEM_PRO=sum(NUMER_QT2_NEM_NEM_PRO, na.rm = TRUE),
                            DENOM_QT2_ALOC_TEMP =sum(DENOM_QT2_ALOC_TEMP, na.rm = TRUE),
                            NUMER_QT3_FREQ_EF=sum(NUMER_QT3_FREQ_EF, na.rm = TRUE),
                            NUMER_QT3_CONCLUIU_EF =sum(NUMER_QT3_CONCLUIU_EF, na.rm = TRUE),
                            NUMER_QT3_CONCLUIU_EF_FREQ_ENSINO_MEDIO=sum(NUMER_QT3_CONCLUIU_EF_FREQ_ENSINO_MEDIO, na.rm = TRUE),
                            NUMER_QT3_CONCLUIU_EF_N_FREQ_ENSINO_MEDIO=sum(NUMER_QT3_CONCLUIU_EF_N_FREQ_ENSINO_MEDIO, na.rm = TRUE),
                            NUMER_QT3_N_FREQ_N_CONCLUIU_EF=sum(NUMER_QT3_N_FREQ_N_CONCLUIU_EF, na.rm = TRUE),
                            NUMER_QT3_FREQ_EM =sum(NUMER_QT3_FREQ_EM, na.rm = TRUE),
                            NUMER_QT3_CONCLUIU_EM=sum(NUMER_QT3_CONCLUIU_EM, na.rm = TRUE),
                            NUMER_QT3_CONCLUIU_EM_FREQ_ENSINO_SUP =sum(NUMER_QT3_CONCLUIU_EM_FREQ_ENSINO_SUP, na.rm = TRUE),
                            NUMER_QT3_CONCLUIU_EM_N_FREQ_ENSINO_SUP=sum(NUMER_QT3_CONCLUIU_EM_N_FREQ_ENSINO_SUP, na.rm = TRUE),
                            NUMER_QT3_N_FREQ_N_CONCLUIU_EM =sum(NUMER_QT3_N_FREQ_N_CONCLUIU_EM, na.rm = TRUE),
                            DENOM_QT3_STATUS_ESCOLAR=sum(DENOM_QT3_STATUS_ESCOLAR, na.rm = TRUE),
                            NUMER_QT4_ATRASO_EF1 =sum(NUMER_QT4_ATRASO_EF1, na.rm = TRUE),
                            DENOM_QT4_MATRICULAS_EF1=sum(DENOM_QT4_MATRICULAS_EF1, na.rm = TRUE),
                            NUMER_QT5_ATRASO_EF2 =sum(NUMER_QT5_ATRASO_EF2, na.rm = TRUE),
                            DENOM_QT5_MATRICULAS_EF2=sum(DENOM_QT5_MATRICULAS_EF2, na.rm = TRUE),
                            NUMER_QT6_ATRASO_EF3 =sum(NUMER_QT6_ATRASO_EF3, na.rm = TRUE),
                            DENOM_QT6_MATRICULAS_EF3=sum(DENOM_QT6_MATRICULAS_EF3, na.rm = TRUE),
                            NUMER_QT7_ATRASO_EF4 =sum(NUMER_QT7_ATRASO_EF4, na.rm = TRUE),
                            DENOM_QT7_MATRICULAS_EF4=sum(DENOM_QT7_MATRICULAS_EF4, na.rm = TRUE),
                            NUMER_QT8_ATRASO_EF5 =sum(NUMER_QT8_ATRASO_EF5, na.rm = TRUE),
                            DENOM_QT8_MATRICULAS_EF5=sum(DENOM_QT8_MATRICULAS_EF5, na.rm = TRUE),
                            NUMER_QT9_ATRASO_EF6 =sum(NUMER_QT9_ATRASO_EF6, na.rm = TRUE),
                            DENOM_QT9_MATRICULAS_EF6=sum(DENOM_QT9_MATRICULAS_EF6, na.rm = TRUE),
                            NUMER_QT10_ATRASO_EF7 =sum(NUMER_QT10_ATRASO_EF7, na.rm = TRUE),
                            DENOM_QT10_MATRICULAS_EF7=sum(DENOM_QT10_MATRICULAS_EF7, na.rm = TRUE),
                            NUMER_QT11_ATRASO_EF8 =sum(NUMER_QT11_ATRASO_EF8, na.rm = TRUE),
                            DENOM_QT11_MATRICULAS_EF8=sum(DENOM_QT11_MATRICULAS_EF8, na.rm = TRUE),
                            NUMER_QT12_ATRASO_EF9 =sum(NUMER_QT12_ATRASO_EF9, na.rm = TRUE),
                            DENOM_QT12_MATRICULAS_EF9=sum(DENOM_QT12_MATRICULAS_EF9, na.rm = TRUE),
                            NUMER_QT13_ATRASO_EM1 =sum(NUMER_QT13_ATRASO_EM1, na.rm = TRUE),
                            DENOM_QT13_MATRICULAS_EM1=sum(DENOM_QT13_MATRICULAS_EM1, na.rm = TRUE),
                            NUMER_QT14_ATRASO_EM2 =sum(NUMER_QT14_ATRASO_EM2, na.rm = TRUE),
                            DENOM_QT14_MATRICULAS_EM2=sum(DENOM_QT14_MATRICULAS_EM2, na.rm = TRUE),
                            NUMER_QT15_ATRASO_EM3 =sum(NUMER_QT15_ATRASO_EM3, na.rm = TRUE),
                            DENOM_QT15_MATRICULAS_EM3=sum(DENOM_QT15_MATRICULAS_EM3, na.rm = TRUE),
                            NUMER_QT16_ATRASO_EM4 =sum(NUMER_QT16_ATRASO_EM4 , na.rm = TRUE),
                            DENOM_QT16_MATRICULAS_EM4=sum(DENOM_QT16_MATRICULAS_EM4, na.rm = TRUE),
                            NUMER_QT9_FREQ_ESC =sum(NUMER_QT9_FREQ_ESC, na.rm = TRUE),
                            DENOM_QT9 =sum(DENOM_QT9, na.rm = TRUE)
                            
                          ),
                          by=list(
                            CAT_ANO=Ano,
                            CAT_TRIMESTRE=Trimestre,
                            CAT_LOCAL=floor(UF/10),
                            CAT_SEXO=V2007,
                            CAT_COR_RACA,
                            CAT_FX_ETARIA,
                            CAT_FX_ETARIA_JUV,
                            CAT_FX_ETARIA_REF_31MARCO,
                            CAT_FX_ETARIA_REF_31MARCO_NO_INPUT,
                            CAT_TIPO_EDUCA,
                            CAT_MOTIVOS,
                            CAT_RDTPC_QUINTIL = CAT_REM_HAB_QUINTIL_REGIAO
                            
                          )    
                          ]
  
  
  ###################################### AGRUPAR POR BRASIL
  
  pesTEMPBRASIL = pesTEMP[,
                          list(
                            SOMA_AMOSTRAS=sum(SOMA_AMOSTRAS, na.rm = TRUE),
                            SOMA_QT_PESSOAS=sum(SOMA_QT_PESSOAS, na.rm = TRUE),
                            NUMER_QT1_N_SABE_LER =sum(NUMER_QT1_N_SABE_LER, na.rm = TRUE),
                            DENOM_QT1=sum(DENOM_QT1, na.rm = TRUE),
                            NUMER_QT2_TRAB_EST =sum(NUMER_QT2_TRAB_EST, na.rm = TRUE),
                            NUMER_QT2_SO_TRAB=sum(NUMER_QT2_SO_TRAB, na.rm = TRUE),
                            NUMER_QT2_SO_ESTUDA =sum(NUMER_QT2_SO_ESTUDA, na.rm = TRUE),
                            NUMER_QT2_NEM_NEM=sum(NUMER_QT2_NEM_NEM, na.rm = TRUE),
                            NUMER_QT2_NEM_NEM_NEM =sum(NUMER_QT2_NEM_NEM_NEM, na.rm = TRUE),
                            NUMER_QT2_NEM_NEM_PRO=sum(NUMER_QT2_NEM_NEM_PRO, na.rm = TRUE),
                            DENOM_QT2_ALOC_TEMP =sum(DENOM_QT2_ALOC_TEMP, na.rm = TRUE),
                            NUMER_QT3_FREQ_EF=sum(NUMER_QT3_FREQ_EF, na.rm = TRUE),
                            NUMER_QT3_CONCLUIU_EF =sum(NUMER_QT3_CONCLUIU_EF, na.rm = TRUE),
                            NUMER_QT3_CONCLUIU_EF_FREQ_ENSINO_MEDIO=sum(NUMER_QT3_CONCLUIU_EF_FREQ_ENSINO_MEDIO, na.rm = TRUE),
                            NUMER_QT3_CONCLUIU_EF_N_FREQ_ENSINO_MEDIO=sum(NUMER_QT3_CONCLUIU_EF_N_FREQ_ENSINO_MEDIO, na.rm = TRUE),
                            NUMER_QT3_N_FREQ_N_CONCLUIU_EF=sum(NUMER_QT3_N_FREQ_N_CONCLUIU_EF, na.rm = TRUE),
                            NUMER_QT3_FREQ_EM =sum(NUMER_QT3_FREQ_EM, na.rm = TRUE),
                            NUMER_QT3_CONCLUIU_EM=sum(NUMER_QT3_CONCLUIU_EM, na.rm = TRUE),
                            NUMER_QT3_CONCLUIU_EM_FREQ_ENSINO_SUP =sum(NUMER_QT3_CONCLUIU_EM_FREQ_ENSINO_SUP, na.rm = TRUE),
                            NUMER_QT3_CONCLUIU_EM_N_FREQ_ENSINO_SUP=sum(NUMER_QT3_CONCLUIU_EM_N_FREQ_ENSINO_SUP, na.rm = TRUE),
                            NUMER_QT3_N_FREQ_N_CONCLUIU_EM =sum(NUMER_QT3_N_FREQ_N_CONCLUIU_EM, na.rm = TRUE),
                            DENOM_QT3_STATUS_ESCOLAR=sum(DENOM_QT3_STATUS_ESCOLAR, na.rm = TRUE),
                            NUMER_QT4_ATRASO_EF1 =sum(NUMER_QT4_ATRASO_EF1, na.rm = TRUE),
                            DENOM_QT4_MATRICULAS_EF1=sum(DENOM_QT4_MATRICULAS_EF1, na.rm = TRUE),
                            NUMER_QT5_ATRASO_EF2 =sum(NUMER_QT5_ATRASO_EF2, na.rm = TRUE),
                            DENOM_QT5_MATRICULAS_EF2=sum(DENOM_QT5_MATRICULAS_EF2, na.rm = TRUE),
                            NUMER_QT6_ATRASO_EF3 =sum(NUMER_QT6_ATRASO_EF3, na.rm = TRUE),
                            DENOM_QT6_MATRICULAS_EF3=sum(DENOM_QT6_MATRICULAS_EF3, na.rm = TRUE),
                            NUMER_QT7_ATRASO_EF4 =sum(NUMER_QT7_ATRASO_EF4, na.rm = TRUE),
                            DENOM_QT7_MATRICULAS_EF4=sum(DENOM_QT7_MATRICULAS_EF4, na.rm = TRUE),
                            NUMER_QT8_ATRASO_EF5 =sum(NUMER_QT8_ATRASO_EF5, na.rm = TRUE),
                            DENOM_QT8_MATRICULAS_EF5=sum(DENOM_QT8_MATRICULAS_EF5, na.rm = TRUE),
                            NUMER_QT9_ATRASO_EF6 =sum(NUMER_QT9_ATRASO_EF6, na.rm = TRUE),
                            DENOM_QT9_MATRICULAS_EF6=sum(DENOM_QT9_MATRICULAS_EF6, na.rm = TRUE),
                            NUMER_QT10_ATRASO_EF7 =sum(NUMER_QT10_ATRASO_EF7, na.rm = TRUE),
                            DENOM_QT10_MATRICULAS_EF7=sum(DENOM_QT10_MATRICULAS_EF7, na.rm = TRUE),
                            NUMER_QT11_ATRASO_EF8 =sum(NUMER_QT11_ATRASO_EF8, na.rm = TRUE),
                            DENOM_QT11_MATRICULAS_EF8=sum(DENOM_QT11_MATRICULAS_EF8, na.rm = TRUE),
                            NUMER_QT12_ATRASO_EF9 =sum(NUMER_QT12_ATRASO_EF9, na.rm = TRUE),
                            DENOM_QT12_MATRICULAS_EF9=sum(DENOM_QT12_MATRICULAS_EF9, na.rm = TRUE),
                            NUMER_QT13_ATRASO_EM1 =sum(NUMER_QT13_ATRASO_EM1, na.rm = TRUE),
                            DENOM_QT13_MATRICULAS_EM1=sum(DENOM_QT13_MATRICULAS_EM1, na.rm = TRUE),
                            NUMER_QT14_ATRASO_EM2 =sum(NUMER_QT14_ATRASO_EM2, na.rm = TRUE),
                            DENOM_QT14_MATRICULAS_EM2=sum(DENOM_QT14_MATRICULAS_EM2, na.rm = TRUE),
                            NUMER_QT15_ATRASO_EM3 =sum(NUMER_QT15_ATRASO_EM3, na.rm = TRUE),
                            DENOM_QT15_MATRICULAS_EM3=sum(DENOM_QT15_MATRICULAS_EM3, na.rm = TRUE),
                            NUMER_QT16_ATRASO_EM4 =sum(NUMER_QT16_ATRASO_EM4, na.rm = TRUE),
                            DENOM_QT16_MATRICULAS_EM4=sum(DENOM_QT16_MATRICULAS_EM4, na.rm = TRUE),
                            NUMER_QT9_FREQ_ESC =sum(NUMER_QT9_FREQ_ESC, na.rm = TRUE),
                            DENOM_QT9 =sum(DENOM_QT9, na.rm = TRUE)
                            
                            
                          ),
                          by=list(
                            CAT_ANO=Ano,
                            CAT_TRIMESTRE=Trimestre,
                            CAT_LOCAL=UF-UF,
                            CAT_SEXO=V2007,
                            CAT_COR_RACA,
                            CAT_FX_ETARIA,
                            CAT_FX_ETARIA_JUV,
                            CAT_FX_ETARIA_REF_31MARCO,
                            CAT_FX_ETARIA_REF_31MARCO_NO_INPUT,
                            CAT_TIPO_EDUCA,
                            CAT_MOTIVOS,
                            CAT_RDTPC_QUINTIL = CAT_REM_HAB_QUINTIL_BRASIL
                            
                          )    
                          ]
  
  #####################################################################
  # UNIR UF, REGIAO E BRASIL
  #####################################################################
  pesTEMPTODO = rbind(pesTEMP_CP_RM_IN,
                      pesTEMP_RM_IN,
                      pesTEMPUF,
                      pesTEMPREGIAO,
                      pesTEMPBRASIL)
  
  
  rm(pesTEMP_CP_RM_IN);rm(pesTEMP_RM_IN);rm(pesTEMPUF); rm(pesTEMPREGIAO);rm(pesTEMPBRASIL)
  
  #####################################################################
  # COLOCAR ROTULOS DA VARIAVEL CATEGORICA LOCAL
  #####################################################################
  pesTEMPTODO$CAT_LOCAL = factor(
    pesTEMPTODO$CAT_LOCAL,
    levels = c(0,
               1,2,3,4,5,
               11,12,13,14,15,16,17,21,22,23,24,25,26,27,28,29,31,32,33,35,41,42,43,50,51,52,53,
               110,120,130,140,150,160,170,210,220,230,240,250,260,270,280,290,310,320,330,350,410,420,430,500,510,520,530,
               111,121,131,141,151,161,171,211,221,231,241,251,261,271,281,291,311,321,331,351,411,421,431,501,511,521,531,
               112,122,132,142,152,162,172,212,222,232,242,252,262,272,282,292,312,322,332,352,412,422,432,502,512,522,532,
               113,123,133,143,153,163,173,213,223,233,243,253,263,273,283,293,313,323,333,353,413,423,433,503,513,523,533
    ),
    labels = c('Brasil',
               'Norte','Nordeste','Sudeste','Sul','Centro-Oeste',
               'Rondônia','Acre','Amazonas','Roraima','Pará','Amapá','Tocantins','Maranhão','Piauí','Ceará','Rio Grande do Norte','Paraíba','Pernambuco','Alagoas','Sergipe','Bahia','Minas Gerais','Espírito Santo','Rio de Janeiro','São Paulo','Paraná','Santa Catarina','Rio Grande do Sul','Mato Grosso do Sul','Mato Grosso','Goiás','Distrito Federal',
               'Porto Velho (RO)','Rio Branco (AC)' ,'Manaus (AM)' ,'Boa Vista (RR)','Belém (PA)','Macapá (AP)' ,'Palmas (TO)' ,'São Luís (MA)' ,'Teresina (PI)' ,'Fortaleza (CE)','Natal (RN)','João Pessoa (PB)','Recife (PE)' ,'Maceió (AL)' ,'Aracaju (SE)','Salvador (BA)' ,'Belo Horizonte (MG)' ,'Vitória (ES)','Rio de Janeiro (RJ)' ,'São Paulo (SP)','Curitiba (PR)' ,'Florianópolis (SC)','Porto Alegre (RS)' ,'Campo Grande (MS)' ,'Cuiabá (MT)' ,'Goiânia (GO)','Brasília (DF)',
               'R.M. menos Porto Velho (RO)','R.M. menos Rio Branco (AC)','R.M. menos Manaus (AM)','R.M. menos Boa Vista (RR)' ,'R.M. menos Belém (PA)' ,'R.M. menos Macapá (AP)','R.M. menos Palmas (TO)','R.M. menos São Luís (MA)','R.M. menos Teresina (PI)','R.M. menos Fortaleza (CE)' ,'R.M. menos Natal (RN)' ,'R.M. menos João Pessoa (PB)' ,'R.M. menos Recife (PE)','R.M. menos Maceió (AL)','R.M. menos Aracaju (SE)' ,'R.M. menos Salvador (BA)','R.M. menos Belo Horizonte (MG)','R.M. menos Vitória (ES)' ,'R.M. menos Rio de Janeiro (RJ)','R.M. menos São Paulo (SP)' ,'R.M. menos Curitiba (PR)','R.M. menos Florianópolis (SC)' ,'R.M. menos Porto Alegre (RS)','R.M. menos Campo Grande (MS)','R.M. menos Cuiabá (MT)','R.M. menos Goiânia (GO)','R.M. menos Brasília (DF)',
               'Região Metropolitana - RO','Região Metropolitana - AC','Região Metropolitana - AM','Região Metropolitana - RR','Região Metropolitana - PA','Região Metropolitana - AP','Região Metropolitana - TO','Região Metropolitana - MA','Região Metropolitana - PI','Região Metropolitana - CE','Região Metropolitana - RN','Região Metropolitana - PB','Região Metropolitana - PE','Região Metropolitana - AL','Região Metropolitana - SE','Região Metropolitana - BA','Região Metropolitana - MG','Região Metropolitana - ES','Região Metropolitana - RJ','Região Metropolitana - SP','Região Metropolitana - PR','Região Metropolitana - SC','Região Metropolitana - RS','Região Metropolitana - MS','Região Metropolitana - MT','Região Metropolitana - GO','Região Metropolitana - DF',
               'Interior - Rondônia','Interior - Acre','Interior - Amazonas','Interior - Roraima','Interior - Pará','Interior - Amapá','Interior - Tocantins','Interior - Maranhão','Interior - Piauí','Interior - Ceará','Interior - Rio Grande do Norte','Interior - Paraíba','Interior - Pernambuco','Interior - Alagoas','Interior - Sergipe','Interior - Bahia','Interior - Minas Gerais','Interior - Espírito Santo','Interior - Rio de Janeiro','Interior - São Paulo','Interior - Paraná','Interior - Santa Catarina','Interior - Rio Grande do Sul','Interior - Mato Grosso do Sul','Interior - Mato Grosso','Interior - Goiás','Interior - Distrito Federal'
  )
  )  
  #####################################################################
  # COLOCAR ROTULOS DA VARIAVEL CATEGORICA SEXO
  #####################################################################
  pesTEMPTODO$CAT_SEXO = factor(
    pesTEMPTODO$CAT_SEXO,
    levels = c(1,2),
    labels = c('Masculino', 'Feminino')
  )
  
  #####################################################################
  # COLOCAR ROTULOS DA VARIAVEL CATEGORICA COR OU RACA
  #####################################################################
  
  pesTEMPTODO$CAT_COR_RACA = factor(
    pesTEMPTODO$CAT_COR_RACA,
    levels = c(1, 2, 3, 4, 5, 9),
    labels = c("Branca", "Preta", "Amarela", "Parda ", "Indígena", "Ignorado")
  )
  
  if(i == 1)
    pesUNION = pesTEMPTODO
  else
    pesUNION = rbind(pesUNION,pesTEMPTODO);
  
  rm(pesTEMPTODO)
  
  
}


#####################################################################
# ESCREVER O ARQUIVO DE SAIDA
# PASSAR O DIRETORIO DE ESCRITA VIA PARAMETRO DE INICIALIZACAO
#####################################################################

dir.create('C:/SIA_DADOS/', showWarnings = FALSE);
dir.create(paste0('C:/SIA_DADOS/', filenamebase), showWarnings = FALSE);
diroutput=file.path(paste0('C:/SIA_DADOS/', filenamebase), gsub(" ","_",gsub(":","",Sys.time())));
dir.create(diroutput, showWarnings = FALSE);
write.csv2(pesUNION, paste0(diroutput, '/', filenamebase, '_output.csv'), row.names = FALSE, na = "");

#####################################################################
# CODIGO VBA QUE GERA OS INDICADORES NA TABEAL DINAMICA
#####################################################################
# ActiveSheet.PivotTables("Tabela dinâmica1").CalculatedFields.Add "IND_PERC_TX_ANALF", _
# "=NUMER_QT1_N_SABE_LER/DENOM_QT1", True
# 
# ActiveSheet.PivotTables("Tabela dinâmica1").CalculatedFields.Add "IND_PERC_TRAB_EST", _
# "=NUMER_QT2_TRAB_EST/DENOM_QT2_ALOC_TEMP", True
# 
# ActiveSheet.PivotTables("Tabela dinâmica1").CalculatedFields.Add "IND_PERC_SO_TRAB", _
# "=NUMER_QT2_SO_TRAB/DENOM_QT2_ALOC_TEMP", True
# 
# ActiveSheet.PivotTables("Tabela dinâmica1").CalculatedFields.Add "IND_PERC_SO_ESTUDA", _
# "=NUMER_QT2_SO_ESTUDA/DENOM_QT2_ALOC_TEMP", True
# 
# ActiveSheet.PivotTables("Tabela dinâmica1").CalculatedFields.Add "IND_PERC_NEM_NEM", _
# "=NUMER_QT2_NEM_NEM/DENOM_QT2_ALOC_TEMP", True
# 
# ActiveSheet.PivotTables("Tabela dinâmica1").CalculatedFields.Add "IND_PERC_NEM_NEM_NEM", _
# "=NUMER_QT2_NEM_NEM_NEM/DENOM_QT2_ALOC_TEMP", True
# 
# ActiveSheet.PivotTables("Tabela dinâmica1").CalculatedFields.Add "IND_PERC_NEM_NEM_PRO", _
# "=NUMER_QT2_NEM_NEM_PRO/DENOM_QT2_ALOC_TEMP", True
# 
# ActiveSheet.PivotTables("Tabela dinâmica1").CalculatedFields.Add "IND_PERC_FREQ_ESC", _
# "=(NUMER_QT9_FREQ_ESC)/(DENOM_QT9)", True
# 
# ActiveSheet.PivotTables("Tabela dinâmica1").CalculatedFields.Add "IND_PERC_FREQ_EF", _
# "=NUMER_QT3_FREQ_EF/DENOM_QT3_STATUS_ESCOLAR", True
# 
# ActiveSheet.PivotTables("Tabela dinâmica1").CalculatedFields.Add "IND_PERC_FREQ_EM", _
# "=NUMER_QT3_FREQ_EM/DENOM_QT3_STATUS_ESCOLAR", True
# 
# ActiveSheet.PivotTables("Tabela dinâmica1").CalculatedFields.Add "IND_PERC_CONT_EST_ENS_MEDIO", _
# "=NUMER_QT3_CONCLUIU_EF_FREQ_ENSINO_MEDIO/DENOM_QT3_STATUS_ESCOLAR", True
# 
# ActiveSheet.PivotTables("Tabela dinâmica1").CalculatedFields.Add "IND_PERC_N_CONT_EST_ENS_MEDIO", _
# "=NUMER_QT3_CONCLUIU_EF_N_FREQ_ENSINO_MEDIO/DENOM_QT3_STATUS_ESCOLAR", True
# 
# ActiveSheet.PivotTables("Tabela dinâmica1").CalculatedFields.Add "IND_PERC_N_FREQ_N_CONCL_EF", _
# "=NUMER_QT3_N_FREQ_N_CONCLUIU_EF/DENOM_QT3_STATUS_ESCOLAR", True
# 
# ActiveSheet.PivotTables("Tabela dinâmica1").CalculatedFields.Add "IND_PERC_CONCLUIU_EF", _
# "=NUMER_QT3_CONCLUIU_EF/DENOM_QT3_STATUS_ESCOLAR", True
# 
# ActiveSheet.PivotTables("Tabela dinâmica1").CalculatedFields.Add "IND_PERC_CONT_EST_ENS_SUP", _
# "=NUMER_QT3_CONCLUIU_EM_FREQ_ENSINO_SUP/DENOM_QT3_STATUS_ESCOLAR", True
# 
# ActiveSheet.PivotTables("Tabela dinâmica1").CalculatedFields.Add "IND_PERC_N_CONT_EST_ENS_SUP", _
# "=NUMER_QT3_CONCLUIU_EM_N_FREQ_ENSINO_SUP/DENOM_QT3_STATUS_ESCOLAR", True
# 
# ActiveSheet.PivotTables("Tabela dinâmica1").CalculatedFields.Add "IND_PERC_N_FREQ_N_CONCL_EM", _
# "=NUMER_QT3_N_FREQ_N_CONCLUIU_EM/DENOM_QT3_STATUS_ESCOLAR", True
# 
# ActiveSheet.PivotTables("Tabela dinâmica1").CalculatedFields.Add "IND_PERC_CONCLUIU_EM", _
# "=NUMER_QT3_CONCLUIU_EM/DENOM_QT3_STATUS_ESCOLAR", True
# 
# ActiveSheet.PivotTables("Tabela dinâmica1").CalculatedFields.Add "IND_PERC_ATRASO_EF_AI", _
# "=(NUMER_QT4_ATRASO_EF1+NUMER_QT5_ATRASO_EF2+NUMER_QT6_ATRASO_EF3+NUMER_QT7_ATRASO_EF4+NUMER_QT8_ATRASO_EF5)/(DENOM_QT4_MATRICULAS_EF1+DENOM_QT5_MATRICULAS_EF2+DENOM_QT6_MATRICULAS_EF3+DENOM_QT7_MATRICULAS_EF4+DENOM_QT8_MATRICULAS_EF5)", True
# 
# ActiveSheet.PivotTables("Tabela dinâmica1").CalculatedFields.Add "IND_PERC_ATRASO_EF_AF", _
# "=(NUMER_QT9_ATRASO_EF6+NUMER_QT10_ATRASO_EF7+NUMER_QT11_ATRASO_EF8+NUMER_QT12_ATRASO_EF9)/(DENOM_QT9_MATRICULAS_EF6+DENOM_QT10_MATRICULAS_EF7+DENOM_QT11_MATRICULAS_EF8+DENOM_QT12_MATRICULAS_EF9)", True
# 
# ActiveSheet.PivotTables("Tabela dinâmica1").CalculatedFields.Add "IND_PERC_ATRASO_EF_EM", _
# "=(NUMER_QT13_ATRASO_EM1+NUMER_QT14_ATRASO_EM2+NUMER_QT15_ATRASO_EM3+NUMER_QT16_ATRASO_EM4)/(DENOM_QT13_MATRICULAS_EM1+DENOM_QT14_MATRICULAS_EM2+DENOM_QT15_MATRICULAS_EM3+DENOM_QT16_MATRICULAS_EM4)", True

