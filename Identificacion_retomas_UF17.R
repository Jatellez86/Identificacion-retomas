

# **********************************************************************************************
#  @Nombre: Identificacion retomas
#  @Autor: Javier Tellez
#  @Fecha: 20230307
#  @Cambios:
#  @Ayudas:
# **********************************************************************************************

library(tidyverse)    # Transformaciones de datos
library(data.table)   # Transformaciones de datos
library(lubridate)    # Tratamiento de fechas
library(httr)         # Publicacion de notificaciones
options(scipen = 999) 

# Ruta de la carpeta donde se encuentran los archivos
vd_zon <- "G:/Unidades compartidas/GM_OP_ANALISIS_DE_DATOS/03 viajes_desglosados"
acciones_zon <- "G:/Unidades compartidas/GM_OP_ANALISIS_DE_DATOS/17 acciones transformado"
retomas_gen <- "G:/Unidades compartidas/GM_OP_ANALISIS_DE_DATOS/19 identificacion retomas"

# Obtener la lista de archivos con el nombre "viajes_desglosado_UF06"
vd_zon_list <- list.files(vd_zon, pattern = "viajes_desglosado_UF17", full.names = T)
acciones_zon_list <- list.files(acciones_zon, pattern = "acciones_regulacion_UF17", full.names = T)
retomas_gen_list <- list.files(retomas_gen, pattern = "retomas_UF17", full.names = T)

# Extraer la fecha del nombre de archivo y crear el data frame
vd_zon_data <- data.table(vd_zon = vd_zon_list) %>% 
  mutate(fecha = as.numeric(str_extract(vd_zon, "[0-9]{8}")))
acciones_zon_data <- data.table(acciones_zon = acciones_zon_list) %>% 
  mutate(fecha = as.numeric(str_extract(acciones_zon, "[0-9]{8}")))
retomas_gen_data <- data.table(retomas = retomas_gen_list) %>% 
  mutate(fecha = as.numeric(str_extract(retomas, "[0-9]{8}")))


data_general <- vd_zon_data %>% 
  left_join(acciones_zon_data, by = "fecha") %>% 
  left_join(retomas_gen_data, by = "fecha") %>% 
  select(fecha, everything()) %>%
  filter(!is.na(vd_zon), !is.na(acciones_zon),
         is.na(retomas)) 

# %>%
#   filter(fecha >= 20220401)

print(data_general$fecha)


#rm()

if (length(data_general$fecha)!=0) {
  
  for(a in 1:length(data_general$fecha)){
    tryCatch({
    
    vd_zon <- fread(data_general$vd_zon[a], skip = "Fecha", strip.white = F) %>%
      select("Fecha" = "Fecha",
             "Operador" = "Operador",
             "Servicio Bus" = "Servicio",
             "Orden Viaje" = "OrdenViaje",
             "Id. Viaje" = "IDViaje",
             "Línea" = "Linea",
             "Coche" = "Coche",
             "Viaje Línea" = "ViajeLinea",
             "Hora Teórica" = "DHoraTeor",
             "Hora Referencia" = "StrLinea",
             "Sublinea" = "Sublinea",
             "Ruta" = "Ruta",
             "Sentido" = "Sentido",
             "Planificado" = "Planificado",
             "Eliminado" = "Eliminado",
             "Estado Motivo" = "StatusMotivo1",
             "Bus" = "Vehiculo",
             "Tipo Bus" = "StrTipoVehiculo1",
             "Kilómetros Teóricos" = "LongitudRuta",
             "Kilómetros Adicionales Autorizados" = "KmsAdicAutorizados",
             "Kilómetros Adicionales No Autorizados" = "KmsAdicNoAutorizados",
             "Kilómetros Eliminados Autorizados" = "KmsElimAutorizados",
             "Kilómetros Eliminados No Autorizados" = "LongitudEliminada",
             "Km. Teóricos No Realizados por Acciones o Desvíos" = "KmsTeoNoRealizadosAccDesv",
             "Dist. Suprimida por  Acciones Justificadas o Desvíos" = "DistSuprAccJustDesv",
             "Dist. Añadida  por  Acciones Justificadas o Desvíos" = "DistAñadAccJustDesv",
             "Porcentaje" = "Porcentaje1",
             "Kilometraje Realizado" = "DistanciaComputable",
             "Puntualidad" = "Puntualidad1",
             "Cumplimiento" = "Cumplimiento",
             "Motivo Adición, eliminación o sustitución de viaje" = "Motivo",
             "Descripción Motivo Adición, eliminación o sustitución de viaje" = "Descripcion",
             "Separación Teórica Anterior" = "SepTeorAnt",
             "Separación Referencia Anterior" = "SepRefAnt",
             "Separación Real Anterior" = "SepRealAnt",
             "Lista Acciones" = "ListaAcciones",
             "Tipo Servicio de  Línea" = "DescripcionTS",
             "Hora Inicio" = "DHoraReal",
             "Hora Final" = "DHoraRealFin",
             "Duración teórica" = "DurTeor",
             "Duración referencia" = "DurRef",
             "Duración Real" = "DurReal",
             "Límite inferior teórico" = "SepTeorAntAnt",
             "Límite superior teórico" = "SepTeorAntPos",
             "Límite inferior referencia" = "SepRefAntAnt",
             "Límite superior referencia" = "SepRefAntPos",
      ) 
    
    vd_2 <- vd_zon %>% 
      janitor::clean_names() %>%
      mutate(kilometros_teoricos = as.numeric(gsub(",", ".", kilometros_teoricos)),
             kilometros_adicionales_autorizados = as.numeric(gsub(",", ".", kilometros_adicionales_autorizados)),
             kilometros_adicionales_no_autorizados = as.numeric(gsub(",", ".", kilometros_adicionales_no_autorizados)),
             kilometros_eliminados_autorizados = as.numeric(gsub(",", ".", kilometros_eliminados_autorizados)),
             kilometros_eliminados_no_autorizados = as.numeric(gsub(",", ".", kilometros_eliminados_no_autorizados)),
             km_teoricos_no_realizados_por_acciones_o_desvios = as.numeric(gsub(",", ".", km_teoricos_no_realizados_por_acciones_o_desvios)),
             dist_suprimida_por_acciones_justificadas_o_desvios = as.numeric(gsub(",", ".", dist_suprimida_por_acciones_justificadas_o_desvios)),
             dist_anadida_por_acciones_justificadas_o_desvios = as.numeric(gsub(",", ".", dist_anadida_por_acciones_justificadas_o_desvios)),
             kilometraje_realizado = as.numeric(gsub(",", ".", kilometraje_realizado))) %>%
      select(serbus = servicio_bus, viaje_linea, ruta, id_viaje, hora_teorica, km_programado = 19,
             km_adicional = 20, planificado, eliminado, hora_teorica, kilometraje_realizado) %>% 
      mutate(hora_inicio_teor = as.numeric(str_sub(hora_teorica, -8L, -7L)) * 3600 +
               as.numeric(str_sub(hora_teorica, -5L, -4L)) * 60 +
               as.numeric(str_sub(hora_teorica, -2L, -1L)))
    
    acciones_reg_zon <- fread(data_general$acciones_zon[a], strip.white = F)
    
    acciones_reg <- acciones_reg_zon %>% 
      janitor::clean_names()
    
    acciones_reg_2 <- acciones_reg %>% # Extraer datos de la columna parametros, según la acción
      filter(str_detect(descripcion_accion, "Introducir Coche|Cambiar Coche|Añadir Viajes")) %>%
      rowwise() %>% 
      mutate(retoma = case_when(str_detect(descripcion_accion, "Introducir Coche") ~
                                  str_sub(parametros, 
                                          str_locate_all(parametros, 'CrearCoche Servicio=')[[1]][2]+3L,
                                          str_locate_all(parametros, 'CrearCoche Servicio=')[[1]][2]+12L),
                                str_detect(descripcion_accion, "Cambiar Coche") ~
                                  str_sub(parametros, 
                                          str_locate_all(parametros, 'Nuevo Servicio=')[[1]][2]+3L,
                                          str_locate_all(parametros, 'Nuevo Servicio=')[[1]][2]+13L),
                                TRUE ~ 
                                  str_sub(parametros, 
                                          str_locate_all(parametros, 'Servicio=')[[1]][2]+3L,
                                          str_locate_all(parametros, 'Servicio=')[[1]][2]+12L)),
             original = case_when(str_detect(descripcion_accion, "Introducir Coche") ~
                                    str_sub(parametros, 
                                            str_locate_all(parametros, 'Referencia Servicio=')[[1]][2] + 3L,
                                            str_locate_all(parametros, 'Referencia Servicio=')[[1]][2] + 12L),
                                  str_detect(descripcion_accion, "Cambiar Coche") ~
                                    str_sub(parametros, 
                                            str_locate_all(parametros, 'Anterior Servicio=')[[1]][2]+3L,
                                            str_locate_all(parametros, 'Anterior Servicio=')[[1]][2]+13L),
                                  TRUE ~
                                    'Reemplazar"'),
             ruta = case_when(str_detect(descripcion_accion, "Introducir Coche") ~
                                str_sub(parametros, 
                                        str_locate_all(parametros, 'Desde Ruta=')[[1]][2] + 3L,
                                        str_locate_all(parametros, 'Desde Ruta=')[[1]][2] + 8L),
                              str_detect(descripcion_accion, "Cambiar Coche") ~
                                str_sub(parametros, 
                                        str_locate_all(parametros, 'Desde Ruta=')[[1]][1] + 13L,
                                        str_locate_all(parametros, 'Desde Ruta=')[[1]][1] + 18L),
                              TRUE ~
                                str_sub(parametros, 
                                        str_locate_all(parametros, 'Ruta=')[[1]][2] + 3L,
                                        str_locate_all(parametros, 'Ruta=')[[1]][2] + 8L)),
             ruta_hasta = case_when(str_detect(descripcion_accion, "Introducir Coche") ~
                                      str_sub(parametros, 
                                              str_locate_all(parametros, 'Hasta Ruta=')[[1]][2] + 3L,
                                              str_locate_all(parametros, 'Hasta Ruta=')[[1]][2] + 8L),
                                    str_detect(descripcion_accion, "Cambiar Coche") ~
                                      str_sub(parametros, 
                                              str_locate_all(parametros, 'Hasta Ruta=')[[1]][1] + 13L,
                                              str_locate_all(parametros, 'Hasta Ruta=')[[1]][1] + 18L),
                                    TRUE ~
                                      str_sub(parametros, 
                                              str_locate_all(parametros, 'Ruta=')[[1]][2] + 3L,
                                              str_locate_all(parametros, 'Ruta=')[[1]][2] + 8L)),
             hora_inicio_teor = case_when(str_detect(descripcion_accion, "Introducir Coche") ~
                                            str_sub(parametros, 
                                                    str_locate_all(parametros, 'HoraIniTeor=')[[1]][2] + 14L,
                                                    str_locate_all(parametros, 'HoraIniTeor=')[[1]][2] + 23L),
                                          str_detect(descripcion_accion, "Cambiar Coche") ~
                                            str_sub(parametros, 
                                                    str_locate_all(parametros, 'HoraIniTeor=')[[1]][1] + 14L,
                                                    str_locate_all(parametros, 'HoraIniTeor=')[[1]][1] + 23L),
                                          TRUE ~
                                            'Reemplazar"'),
             viaje_desde_orig = if_else(str_detect(descripcion_accion, "Introducir Coche"),
                                        str_sub(parametros, 
                                                str_locate_all(parametros, 'ViajeLinea=')[[1]][1] + 13L,
                                                str_locate_all(parametros, 'ViajeLinea=')[[1]][1] + 17L),
                                        "999\""),
             viaje_hasta_orig = if_else(str_detect(descripcion_accion, "Introducir Coche"),
                                        str_sub(parametros, 
                                                str_locate_all(parametros, 'ViajeLinea=')[[1]][2] + 13L,
                                                str_locate_all(parametros, 'ViajeLinea=')[[1]][2] + 17L),
                                        "999\""),
             viaje_desde_ret = if_else(str_detect(descripcion_accion, "Añadir Viajes"),
                                       str_sub(parametros, 
                                               str_locate_all(parametros, 'ViajeLinea=')[[1]][1] + 13L,
                                               str_locate_all(parametros, 'ViajeLinea=')[[1]][1] + 17L),
                                       "1\""),
             viaje_hasta_ret = if_else(str_detect(descripcion_accion, "Añadir Viajes"),
                                       str_sub(parametros, 
                                               str_locate_all(parametros, 'ViajeLinea=')[[1]][1] + 13L,
                                               str_locate_all(parametros, 'ViajeLinea=')[[1]][1] + 17L),
                                       "1\""),
             id_desde_orig = case_when(str_detect(descripcion_accion, "Introducir Coche") ~
                                         str_sub(parametros, 
                                                 str_locate_all(parametros, 'IdViaje=')[[1]][1] + 10L,
                                                 str_locate_all(parametros, 'IdViaje=')[[1]][1] + 13L),
                                       str_detect(descripcion_accion, "Cambiar Coche") ~
                                         str_sub(parametros, 
                                                 str_locate_all(parametros, ' Viaje=')[[1]][1] + 9L,
                                                 str_locate_all(parametros, ' Viaje=')[[1]][1] + 12L),
                                       TRUE ~
                                         "999\""),
             id_hasta_orig = case_when(str_detect(descripcion_accion, "Introducir Coche") ~
                                         str_sub(parametros, 
                                                 str_locate_all(parametros, 'IdViaje=')[[1]][2] + 10L,
                                                 str_locate_all(parametros, 'IdViaje=')[[1]][2] + 13L),
                                       str_detect(descripcion_accion, "Cambiar Coche") ~
                                         str_sub(parametros, 
                                                 str_locate_all(parametros, ' Viaje=')[[1]][2] + 9L,
                                                 str_locate_all(parametros, ' Viaje=')[[1]][2] + 12L),
                                       TRUE ~
                                         "999\"")) %>% 
      mutate(retoma = str_sub(retoma, str_locate_all(retoma, "\"")[[1]][2]+1L, -1L),
             original = str_sub(original, str_locate_all(original, "\"")[[1]][2]+1L, -1L),
             ruta = as.numeric(str_sub(ruta, str_locate_all(ruta, "\"")[[1]][2]+1L, -1L)),
             ruta_hasta = as.numeric(str_sub(ruta_hasta, str_locate_all(ruta_hasta, "\"")[[1]][2]+1L, -1L)),
             hora_inicio_teor = as.character(str_sub(hora_inicio_teor, str_locate_all(hora_inicio_teor, "\"")[[1]][2]+1L, -1L)),
             viaje_desde_orig = as.numeric(str_sub(viaje_desde_orig, 1L, str_locate(viaje_desde_orig, '"')[1] - 1L)),
             viaje_hasta_orig = as.numeric(str_sub(viaje_hasta_orig, 1L, str_locate(viaje_hasta_orig, '"')[1] - 1L)),
             viaje_desde_ret = as.numeric(str_sub(viaje_desde_ret, 1L, str_locate(viaje_desde_ret, '"')[1] - 1L)),
             viaje_hasta_ret = as.numeric(str_sub(viaje_hasta_ret, 1L, str_locate(viaje_hasta_ret, '"')[1] - 1L)),
             id_desde_orig = as.numeric(str_extract(id_desde_orig, '\\d+')),
             id_hasta_orig = as.numeric(str_extract(id_desde_orig, '\\d+'))) %>% 
      mutate(hora_inicio_seg = as.numeric(str_sub(hora_inicio_teor, -8L, -7L)) * 3600 +
               as.numeric(str_sub(hora_inicio_teor, -5L, -4L)) * 60 +
               as.numeric(str_sub(hora_inicio_teor, -2L, -1L))) %>% 
      ungroup()
    
    acc_reg_2_anade_v <- acciones_reg_2 
    
    for(i in 1:length(acciones_reg_2$descripcion_accion)){
      if(acciones_reg_2$descripcion_accion[i] == "Añadir Viajes"){
        
        acc_reg_2_anade_v$original[i] <- (acciones_reg_2 %>% 
                                            filter(retoma == acc_reg_2_anade_v$retoma[i]))$original[1]
        acc_reg_2_anade_v$hora_inicio_teor[i] <- "23:59:59"
        acc_reg_2_anade_v$viaje_desde_orig[i] <- (acciones_reg_2 %>% 
                                                    filter(retoma == acc_reg_2_anade_v$retoma[i]))$viaje_hasta_orig[1] + 1
        acc_reg_2_anade_v$viaje_hasta_orig[i] <- acc_reg_2_anade_v$viaje_desde_orig[i]
        acc_reg_2_anade_v$viaje_desde_ret[i] <- (acciones_reg_2 %>% 
                                                   filter(retoma == acc_reg_2_anade_v$retoma[i]))$viaje_desde_ret[2] + 1
        acc_reg_2_anade_v$viaje_hasta_ret[i] <- acc_reg_2_anade_v$viaje_desde_ret[i]
        acc_reg_2_anade_v$id_desde_orig[i] <- (acciones_reg_2 %>% 
                                                 filter(retoma == acc_reg_2_anade_v$retoma[i]))$id_hasta_orig[1] + 1
        acc_reg_2_anade_v$id_hasta_orig[i] <- acc_reg_2_anade_v$id_desde_orig[i]
        acc_reg_2_anade_v$hora_inicio_seg[i] <- 0
        #print(acc_reg_2_anade_v$original[i])
      }
      #print(acciones_reg_2$descripcion_accion[i])
    }
    print("blque 1 ok")
    acciones_reg_2_mod <- acc_reg_2_anade_v %>%
      left_join(vd_2 %>% # Identificación de un id_viaje inicial por hora de inicio teorica de servicio original
                  select(serbus, hora_inicio_teor, id_desde_orig_mod = id_viaje, ruta),
                by = c("original" = "serbus", "hora_inicio_seg" = "hora_inicio_teor", "ruta")) %>%
      left_join(vd_2 %>%  # Identificación de un id_viaje inicial por viaje_linea inicial de servicio original
                  select(serbus, viaje_linea, id_desde_orig_mod_2 = id_viaje, ruta),
                by = c("original" = "serbus", "viaje_desde_orig" = "viaje_linea", "ruta")) %>%
      mutate(id_desde_orig_mod = if_else(!is.na(id_desde_orig_mod_2), # Consolidación de id_viaje inicial servicio original
                                         as.numeric(id_desde_orig_mod_2),
                                         as.numeric(id_desde_orig_mod))) %>% 
      mutate(id_hasta_orig = if_else(id_hasta_orig == id_desde_orig & id_desde_orig != id_desde_orig_mod, # Posible cambio 
                                     id_desde_orig_mod,                       #de id_viaje final(hasta) servicio original
                                     id_hasta_orig),
             id_desde_orig = id_desde_orig_mod) %>%  # Selección definitiva de id_viaje inicial servicio original
      select(-c(hora_inicio_seg, id_desde_orig_mod, id_desde_orig_mod_2)) %>%
      left_join(vd_2 %>% 
                  select(serbus, viaje_linea, id_hasta_orig_mod = id_viaje, ruta),
                by = c("original" = "serbus", "viaje_hasta_orig" = "viaje_linea", "ruta_hasta" = "ruta")) %>%
      mutate(id_hasta_orig = if_else(!is.na(id_hasta_orig_mod) & id_hasta_orig != id_hasta_orig_mod,
                                     as.numeric(id_hasta_orig_mod),
                                     id_hasta_orig)) %>% 
      select(-id_hasta_orig_mod) %>% 
      group_by(original, hora_inicio_teor) %>% 
      filter(id_desde_orig == max(id_desde_orig, na.rm = T)) %>% #filtro de viajes en caso de viajes duplicados en vd
      ungroup() %>%                                             # con distinto id_viaje pero con igual hora teorica inicial
      left_join(vd_2 %>%  # Primera identificación de id_viaje inicial para retoma de acuerdo al viaje_linea
                  select(serbus, viaje_linea, id_viaje, ruta), 
                by = c("retoma" = "serbus", "viaje_desde_ret" = "viaje_linea", "ruta")) %>% 
      mutate(id_desde_ret = id_viaje,
             id_hasta_ret = id_viaje,
             consec_ini = 0, # Columna auxiliar dado que los viajes en viajes desglosados pueden tener id_desordenados
             consec_fin = 0, # Columna auxiliar dado que los viajes en viajes desglosados pueden tener id_desordenados
             viajes = 0) # Columna auxiliar de viajes
    print("blque 2 ok")
    for(i in 1:length(acciones_reg_2_mod$original)){ # Conteo de cantidad de viajes por cada retoma o sustituyente
      
      vd_3 <- vd_2 %>% 
        filter(serbus == acciones_reg_2_mod$original[i]) %>% 
        mutate(n = 1:n())
      
      acciones_reg_2_mod$consec_ini[i] <- (vd_3 %>% 
                                             filter(id_viaje == acciones_reg_2_mod$id_desde_orig[i]))$n[1]
      acciones_reg_2_mod$consec_fin[i] <- (vd_3 %>% 
                                             filter(id_viaje == acciones_reg_2_mod$id_hasta_orig[i]))$n[1]
      if(is.na(acciones_reg_2_mod$consec_fin[i])){
        acciones_reg_2_mod$consec_fin[i] <- (vd_3 %>% 
                                               filter(n > acciones_reg_2_mod$consec_ini[i] & id_viaje <= acciones_reg_2_mod$id_hasta_orig[i]) %>% 
                                               filter(n == max(n, na.rm = T)))$n[1]
      }
      acciones_reg_2_mod$consec_fin[i] <- if_else(is.na(acciones_reg_2_mod$consec_fin[i]),
                                                  acciones_reg_2_mod$consec_ini[i],
                                                  acciones_reg_2_mod$consec_fin[i])
      acciones_reg_2_mod$viajes[i] <- acciones_reg_2_mod$consec_fin[i] - acciones_reg_2_mod$consec_ini[i] + 1
      
    }
    
    acciones_reg_3 <- acciones_reg_2_mod %>% 
      select(retoma, original, viaje_desde_ret, viaje_hasta_ret, id_desde_orig, id_hasta_orig, viajes)
    print("blque 3 ok")
    for(i in 1:length(acciones_reg_3$viajes)){ # Generación de filas adic segun cantidad de viajes por retoma o sustiyente
      if(acciones_reg_3$viajes[i] > 1){
        for(y in 1:acciones_reg_3$viajes[i]){
          acciones_reg_3 <- acciones_reg_3 %>% 
            add_row(retoma = acciones_reg_3$retoma[i],
                    original = acciones_reg_3$original[i],
                    viaje_desde_ret = acciones_reg_3$viaje_desde_ret[i] + y -1,
                    viaje_hasta_ret = acciones_reg_3$viaje_desde_ret[i] + y -1,
                    id_desde_orig = acciones_reg_3$id_desde_orig[i],
                    id_hasta_orig = acciones_reg_3$id_hasta_orig[i],
                    viajes = 1)
        }
      }
    }
    
    acciones_reg_3_ret_mod <- acciones_reg_3 %>% # Identificación de id_viaje definitivo de cada retoma según su viaje_linea
      filter(viajes == 1) %>% 
      left_join(vd_2 %>% 
                  select(serbus, viaje_linea, id_viaje),
                by = c("retoma" = "serbus", "viaje_desde_ret" = "viaje_linea")) %>% 
      mutate(id_desde_ret = id_viaje,
             id_hasta_ret = id_viaje) %>%
      group_by(retoma, viaje_desde_ret) %>% 
      filter(id_viaje == max(id_viaje)) %>% # Se eliminan viajes con viaje_linea repetido pero diferente id_viaje 
      ungroup() %>% 
      select(-c(viaje_desde_ret, viaje_hasta_ret, viajes, id_viaje)) %>%
      group_by(retoma, id_desde_orig) %>% 
      mutate(n = 1:n()) %>% 
      ungroup()
    
    for(i in 1:length(acciones_reg_3_ret_mod$original)){
      
      vd_3 <- vd_2 %>%
        filter(serbus == acciones_reg_3_ret_mod$original[i]) %>%
        mutate(n_inicial = 1:n()) %>% 
        mutate(desde = if_else(id_viaje == acciones_reg_3_ret_mod$id_desde_orig[i], T, F),
               hasta = if_else(id_viaje == acciones_reg_3_ret_mod$id_hasta_orig[i], T, F),
               limites = desde | hasta,
               acumula = 0) # Columna auxiliar
      
      for(j in 1:length(vd_3$limites)){
        vd_3$acumula[j] <- sum(vd_3$limites[1:j])
        if(j>2){
          if(vd_3$acumula[j] == 2  & sum(vd_3$limites[1:j-1]) == 2){
            vd_3$acumula[j] <- 0
          }
        }
      }
      
      vd_3 <- vd_3 %>% 
        filter(acumula == 1 | acumula == 2) %>% 
        mutate(n = 1:n()) %>% 
        filter(n == acciones_reg_3_ret_mod$n[i])
      
      acciones_reg_3_ret_mod$id_desde_orig[i] <- vd_3$id_viaje[1]
      acciones_reg_3_ret_mod$id_hasta_orig[i] <- vd_3$id_viaje[1]
      
    }
    
    acciones_reg_4 <- acciones_reg_3_ret_mod %>% 
      mutate(retoma = str_c(retoma, "__", id_desde_ret),
             original = str_c(original, "__", id_desde_orig)) %>% 
      select(retoma, original) %>% 
      na.omit()
    
    retomas <- acciones_reg_4
    
    for(i in 1:10){
      retomas <- retomas %>% 
        left_join(retomas %>% 
                    select(1,2), by = c("original" = "retoma"))
      
      names <- names(retomas)
      
      colnames(retomas)[str_which(names, "original$")] <- str_c("original_", i)
      colnames(retomas)[str_which(names, "y$")] <- "original"
      
      if(sum(is.na(retomas[length(retomas)])) == nrow(retomas)){
        break
      }
    }
    
    retomas <- retomas %>% 
      select(-length(retomas)) %>% 
      rename(original = 2)
    
    for(i in 1:length(retomas$retoma)){
      for(j in 2:length(retomas)){
        if(is.na(retomas[i,j])){
          retomas[i,j] <- retomas[i,(j-1L)] 
        }
      }
    }
    
    retomas <- retomas %>% 
      select(1, length(retomas)) %>% 
      rename(original = 2)
    
    retomas_def <- retomas %>%
      rowwise() %>% 
      mutate(id_retoma = as.numeric(str_sub(retoma, str_locate(retoma, "__")[1] + 2L, nchar(retoma))),
             id_orig = as.numeric(str_sub(original, str_locate(original, "__")[1] + 2L, nchar(original))),
             serbus_ret = str_sub(retoma, 1L, str_locate(retoma, "__") - 1L)[1],
             serbus_orig = str_sub(original, 1L, str_locate(original, "__")[1] - 1L)) %>% 
      select(serbus_ret, id_retoma, serbus_orig, id_orig) %>% 
      ungroup() %>% 
      left_join(vd_2 %>% # Km efectivamente ejecutado retoma
                  select(serbus, id_viaje, km_efectivamente_ejecutado = kilometraje_realizado,
                         planificado, eliminado, hora_teorica),
                by = c("serbus_ret" = "serbus", "id_retoma" = "id_viaje")) %>% 
      left_join(vd_2 %>% # Km efectivamente ejecutado original
                  select(serbus, id_viaje, km_efectivamente_ejecutado = kilometraje_realizado,
                         ruta, planificado, eliminado, hora_teorica),
                by = c("serbus_orig" = "serbus", "id_orig" = "id_viaje"),
                suffix = c("_ret", "_orig")) %>% 
      left_join(vd_2 %>% # Km programado original según rutasae
                  group_by(ruta) %>% 
                  summarise(km_programado = max(km_programado, na.rm = T)) %>% 
                  ungroup(),
                by = "ruta") %>%
      replace_na(list(km_efectivamente_ejecutado_ret = 0,
                      km_efectivamente_ejecutado_orig = 0)) %>% 
      mutate(ret_mal_creada = if_else(km_efectivamente_ejecutado_ret + km_efectivamente_ejecutado_orig >
                                        km_programado + 0.01,
                                      T, F),
             debio_eliminarse_orig = if_else(km_efectivamente_ejecutado_orig == 0 & planificado_orig == "Planificado"
                                             & eliminado_orig == "",
                                             T, F),
             debio_eliminarse_ret = if_else(km_efectivamente_ejecutado_ret == 0 & eliminado_ret == "",
                                            T, F)) %>% 
      # filter(!str_detect(serbus_ret, "AD")) %>% 
      filter(!(km_efectivamente_ejecutado_orig == 0 & km_efectivamente_ejecutado_ret == 0 &
                 eliminado_ret == "Eliminado")) %>%
      # filter(!(km_efectivamente_ejecutado_ret == 0 & km_efectivamente_ejecutado_orig != 0)) %>% 
      group_by(serbus_orig, id_orig) %>%
      mutate(rep = 1:n(),
             rep = max(rep, na.rm = T)) %>% 
      ungroup() %>% 
      select(serbus_orig, id_orig, serbus_ret, id_retoma, ruta, km_programado,
             km_efectivamente_ejecutado_orig, km_efectivamente_ejecutado_ret, 
             planificado_orig, eliminado_orig, hora_teorica_orig, planificado_ret,
             eliminado_ret, hora_teorica_ret, everything())
    
    write.csv(retomas_def, str_c("G:/Unidades compartidas/GM_OP_ANALISIS_DE_DATOS/19 identificacion retomas/", data_general$fecha[a], "_retomas_UF17.csv"), row.names = F)
    print(data_general$fecha[a])
    
    }, error = function(e) {
      print(paste0("Error en fecha # ", data_general$fecha[a], " ", a, ": ", e))
      
      
    })
    
  }
  
  
}

print("proceso finalizado")


#write.xlsx(acciones_reg_2, file = 'C:/Users/javier.tellez/Downloads/acciones_reg_2.xlsx')




