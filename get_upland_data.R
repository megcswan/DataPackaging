#this script pulls views and tables from the SCPN_UplandEvent DB for annual data package updates
#following data collection and validation.

#load packages
library(DBI)
library(tidyverse)
#getwd()
setwd("Z:\\DataManagement\\DataPackages\\SCPN_UplandVegetation\\SCPN_UplandVegetation_2007-2024\\DataFiles")
#open connection to database
conUpE <- DBI::dbConnect(odbc::odbc(),
                         Driver   = "ODBC Driver 17 for SQL Server",
                         Server   = "INPSCPNMS01\\CLPSandbox",
                         Database = "SCPN_UplandEvent",
                         Trusted_Connection = "yes",
                         Port     = 1433
)
##get all views needed for package
dbsqlview <- read.table(conUpE, tableType = "VIEW", schema = "dbo")
#########################################################################################################
#get event table to publish
event <- dbReadTable(conUpE, "view_Event_Sampled")%>%
  select(Park, EcoSite, Plot, EventYear, EventPanel, EventDate)
#do some checks
event%>%
  select(Park, EcoSite)%>%
  distinct
event%>%
  select(EventYear)%>%
  distinct()%>%
  arrange(EventYear)
#write event to datafiles
write.csv(event, "view_Event_Sampled_2007-2024.csv", row.names = F)
#######################################################################################################
#get soil stability table to publish
soil.stab<- dbReadTable(conUpE, "view_SoilStability_All", as.is=FALSE, 
                        stringsAsFactors = FALSE)

#select desired fields
soil.stab.data<-soil.stab%>%
  select(Park, EcoSite, Plot, EventYear, TransectLetter, PositionIdentifier, 
         PositionAlongTransect_m, DominantVegetationType, StabilityRatingClass)
#check number of years--no data collected in 2020
soil.stab.data%>%
  select(EventYear)%>%
  distinct()%>%
  arrange(EventYear)
#check parks and ecosites
soil.stab.data%>%
  select(Park, EcoSite)%>%
  distinct()
#check for blanks or NA's
nacheck<-soil.stab.data%>%
  filter_all(any_vars(is.na(.)))
#fix existing
soil.stab.data<-soil.stab.data%>%
  replace_na(list(DominantVegetationType ="NAN", StabilityRatingClass = "NAN"))
#recheck for NA's
nacheck<-soil.stab.data%>%
  filter_all(any_vars(is.na(.)))
#change NA positions
soil.stab.data<-soil.stab.data%>%
  mutate(PositionAlongTransect_m=ifelse(
    is.na(PositionAlongTransect_m),"-99999", PositionAlongTransect_m))
#check one more time
nacheck<-soil.stab.data%>%
  filter_all(any_vars(is.na(.)))
str(soil.stab.data)

#write soil stablity table to datafiles
write.csv(soil.stab.data, "view_SoilStability_All_2007-2024.csv", row.names = F)

#####################################################################################################
#get surface feature data to publish
surf.fea<- dbReadTable(conUpE, "view_Event_Sampled", as.is=FALSE, 
                       stringsAsFactors = FALSE)
#collected this data only 2007-2019. Filter out other years
surf.fea.data<-surf.fea%>%
  select(Park, EcoSite, Plot, EventYear, TransectLetter, QuadratNumber, SurfaceFeaturesCollected,
         SurfaceFeature, CoverClassMidpoint_Quadrat_pct, CoverClass)%>%
  filter(EventYear<2021)
#check number of years
surf.fea.data%>%
  select(EventYear)%>%
  distinct()%>%
  arrange(EventYear)
#check parks and ecosites
surf.fea.data%>%
  select(Park, EcoSite)%>%
  distinct()
#check for blanks or NA's
nacheck<-surf.fea.data%>%
  filter_all(any_vars(is.na(.)))
#fix existing
str(surf.fea.data)

surf.fea.data<-surf.fea.data%>%
  mutate(CoverClass=as.character(CoverClass))%>%
  mutate(CoverClassMidpoint_Quadrat_pct=ifelse(is.na(CoverClassMidpoint_Quadrat_pct), "-99999",
                                               CoverClassMidpoint_Quadrat_pct))%>%
  replace_na(list(CoverClass ="NAN"))
#recheck
nacheck<-surf.fea.data%>%
  filter_all(any_vars(is.na(.)))

write.csv(surf.fea.data, "view_SurfaceFeatures_All_2007-2020.csv", row.names = F)
#######################################################################################################
#functional groups

fxn.all<-dbReadTable(conUpE, "view_FunctionalGroups_All", as.is=F, 
                     stringsAsFactors=F)
#select desired fields and remove tree understory which was collected only in 2007
fxn.all.data<-fxn.all%>%
  select(Park, EcoSite, Plot, EventYear, TransectLetter, QuadratNumber,
         FunctionalGroupsCollected, FunctionalGroupsPresent, FunctionalGroup,
         CoverClassMidpoint_Quadrat_pct, CoverClass)%>%
  filter(!FunctionalGroup %in% "TreeUnderstoryCoverClass_10m")

#check number of years
fxn.all.data%>%
  select(EventYear)%>%
  distinct()%>%
  arrange(EventYear)
#check parks and ecosites
fxn.all.data%>%
  select(Park, EcoSite)%>%
  distinct()
#check for blanks or NA's
nacheck<-fxn.all.data%>%
  filter_all(any_vars(is.na(.)))

fxn.all.data<-fxn.all.data%>%
  mutate(CoverClass=as.character(CoverClass))%>%
  mutate(FunctionalGroupsPresent=as.character(FunctionalGroupsPresent))%>%
  mutate(CoverClassMidpoint_Quadrat_pct=ifelse(is.na(CoverClassMidpoint_Quadrat_pct), "-99999",
                                               CoverClassMidpoint_Quadrat_pct))%>%
  replace_na(list(CoverClass ="NAN", FunctionalGroupsPresent = "NAN", FunctionalGroup = "NAN"))
#recheck
nacheck<-fxn.all.data%>%
  filter_all(any_vars(is.na(.)))

write.csv(fxn.all.data, "view_FunctionGroups_All_2007-2024.csv", row.names = F)


#########################################################################################################
#species cover
spp.all<-dbReadTable(conUpE, "vtbl_NestedSpecies_All")
#get species that occur in each ecosite only to reduce size
spp.all.occur<-spp.all%>%
  group_by(EcoSite, CurrentSpecies)%>%
  mutate(InEcosite = if_else(sum(SpeciesPresentInQuadratForNested)>0, 1, 0))%>%
  ungroup()%>%
  filter(InEcosite >0, EventPanel %in% c("A", "B", "C"))%>%
  filter(!NestedSpeciesValidation %in% "Issue")%>%
  separate(TransectQuadratID, into = c(NA, NA, NA, "TransectLetter", "QuadratNumber"), sep = "_")%>%
  select(Park, EcoSite, Plot, TransectLetter, QuadratNumber, EventYear, CurrentSpecies, 
         SpeciesPresentInQuadratForCover, 
         SpeciesPresentInQuadratForNested, UsableQuadratForCover,
         UsableQuadratForNested, CommonName, Lifeform, Duration, 
         NestedQuadratSizeClass, CoverClassMidpoint_Quadrat_pct)

#check for NA
nacheck<-spp.all.occur%>%
  filter_all(any_vars(is.na(.)))
#change existing NA to NAN
spp.all.occur<-spp.all.occur%>%
  mutate(NestedQuadratSizeClass= as.character(NestedQuadratSizeClass))%>%
  mutate(CoverClassMidpoint_Quadrat_pct= as.numeric(CoverClassMidpoint_Quadrat_pct))%>%
  replace_na(list(CommonName = "NAN", Lifeform = "NAN", Duration = "NAN", 
                  NestedQuadratSizeClass = "NAN"))

spp.all.occur<-spp.all.occur%>%
  mutate(CoverClassMidpoint_Quadrat_pct=ifelse(is.na(CoverClassMidpoint_Quadrat_pct), 
                                               "-99999", CoverClassMidpoint_Quadrat_pct))

nacheck<-spp.all.occur%>%
  filter_all(any_vars(is.na(.)))

write.csv(spp.all.occur, "view_NestedSpecies_All_2007-2024.csv", row.names=F)
########################################################################################################

#species plot summaries

spp.summary<- dbReadTable(conUpE, "vtbl_NestedSpeciesSummary_ParkEcoPlot")
#select desired fields
spp.summary<-spp.summary%>%
  select(Park, EcoSite, Plot, EventYear, CurrentSpecies, CommonName, Nativity, Lifeform, Duration,
         CoverClassMidpointMean_Plot_pct, CoverClassMidpointSD_Plot_pct, 
         UsableQuadratsForCover_cnt, UsableQuadratsForNested_cnt, SpeciesPresentInQuadratForCover_cnt,
         SpeciesPresentInQuadratForNested_cnt)

#check for NAs
nacheck<-spp.summary%>%
  filter_all(any_vars(is.na(.)))

#change existing NA to NAN
spp.summary<-spp.summary%>%
  replace_na(list(CommonName = "NAN", Lifeform = "NAN", Duration = "NAN", Nativity = "NAN"))

nacheck<-spp.summary%>%
  filter_all(any_vars(is.na(.)))

write.csv(spp.summary, "vtbl_NestedSpeciesSummary_ParkEcoPlot_2007-2024.csv", row.names = F)
#############################################################################################
#Seedlings
seedlings.all<-dbReadTable(conUpE, "view_Seedling_All")
#select desired fields and remove tree understory which was collected only in 2007
seedlings.all.data<-seedlings.all%>%
  select(Park, EcoSite, Plot, EventYear, TransectLetter, QuadratNumber,
         SeedlingsCollected, SeedlingsPresent, SpeciesID, FieldID,
         SeedlingSizeGroup, SeedlingCount)

#check number of years
seedlings.all.data%>%
  select(EventYear)%>%
  distinct()%>%
  arrange(EventYear)
#check parks and ecosites
seedlings.all.data%>%
  select(Park, EcoSite)%>%
  distinct()
#check for blanks or NA's
nacheck<-seedlings.all.data%>%
  filter_all(any_vars(is.na(.)))

seedlings.all.data<-seedlings.all.data%>%
  mutate(SeedlingsPresent=as.character(SeedlingsPresent))%>%
  mutate(SeedlingsCollected=as.character(SeedlingsCollected))%>%
  mutate(SeedlingCount=ifelse(is.na(SeedlingCount),  "-99999", SeedlingCount))%>%
  replace_na(list(SeedlingsCollected ="NAN", SeedlingsPresent = "NAN"))
#recheck
nacheck<-seedlings.all.data%>%
  filter_all(any_vars(is.na(.)))

write.csv(seedlings.all.data, "view_Seedlings_All_2007-2024.csv", row.names = F)
########################################################################################################
#saplings
saplings.all<-dbReadTable(conUpE, "view_Sapling_All")
#select desired fields and remove tree understory which was collected only in 2007
saplings.all.data<-saplings.all%>%
  select(Park, EcoSite, Plot, EventYear, SaplingsCollected, SaplingsPresent, SpeciesID, FieldID,
         SaplingSizeGroup, SaplingCount)

#check number of years
saplings.all.data%>%
  select(EventYear)%>%
  distinct()%>%
  arrange(EventYear)
#check parks and ecosites
saplings.all.data%>%
  select(Park, EcoSite)%>%
  distinct()
#check for blanks or NA's
nacheck<-saplings.all.data%>%
  filter_all(any_vars(is.na(.)))

saplings.all.data<-Saplings.all.data%>%
  mutate(SaplingsPresent=as.character(SaplingsPresent))%>%
  mutate(SaplingsCollected=as.character(SaplingsCollected))%>%
  mutate(SaplingCount=ifelse(is.na(SaplingCount),  "-99999", SaplingCount))%>%
  replace_na(list(SaplingsCollected ="NAN", SaplingsPresent = "NAN"))
#recheck
nacheck<-saplings.all.data%>%
  filter_all(any_vars(is.na(.)))

write.csv(saplings.all.data, "view_Saplings_All_2007-2024.csv", row.names = F)

########################################################################################################
#trees
trees.all<-dbReadTable(conUpE, "view_TreeBasalAreaAndStatus_All")
#select desired fields and remove tree understory which was collected only in 2007
trees.all.data<-trees.all%>%
  select(Park, EcoSite, Plot, EventYear, Tag, SpeciesID, FieldID, FieldID, TreeStatus, 
         TreeBasalArea_sqmeter, StemCount)

#check number of years
trees.all.data%>%
  select(EventYear)%>%
  distinct()%>%
  arrange(EventYear)
#check parks and ecosites
trees.all.data%>%
  select(Park, EcoSite)%>%
  distinct()
#check for blanks or NA's
nacheck<-trees.all.data%>%
  filter_all(any_vars(is.na(.)))

#trees.all.data<-trees.all.data%>%
#  mutate(SaplingsPresent=as.character(SaplingsPresent))%>%
#  mutate(SaplingsCollected=as.character(SaplingsCollected))%>%
#  mutate(SaplingCount=ifelse(is.na(SaplingCount),  "-99999", SaplingCount))%>%
#  replace_na(list(SaplingsCollected ="NAN", SaplingsPresent = "NAN"))
#recheck
nacheck<-trees.all.data%>%
  filter_all(any_vars(is.na(.)))

write.csv(trees.all.data, "view_TreeBasalAreaAndStatus_2007-2024.csv", row.names = F)



########################################################################################################
#canopy closure & canopy cover
dbReadTable(conUpE, "view_CanopyCover_All")
#select desired fields and remove tree understory which was collected only in 2007
canopy.all.data<-canopy.all%>%
  select(Park, EcoSite, Plot, EventYear, CanopyCoverStartPosition_cm, CanopyCoverEndPosition_cm, 
         CanopyCoverLength_Segment_cm)

#check number of years
canopy.all.data%>%
  select(EventYear)%>%
  distinct()%>%
  arrange(EventYear)
#check parks and ecosites
canopy.all.data%>%
  select(Park, EcoSite)%>%
  distinct()
#check for blanks or NA's
nacheck<-canopy.all.data%>%
  filter_all(any_vars(is.na(.)))

canopy.all.data<-canopy.all.data%>%
  replace_na(list(CanopyCoverEndPosition_cm =-99999, CanopyCoverLength_Segment_cm = -99999))
#recheck
nacheck<-canopy.all.data%>%
  filter_all(any_vars(is.na(.)))

write.csv(canopy.all.data, "view_CanopyCover_All_2007-2024.csv", row.names = F)


###############################################################################################
#canopy closure
canopy.clos.all<-dbReadTable(conUpE, "view_CanopyClosure_All")
#select desired fields and remove tree understory which was collected only in 2007
canopy.clos.all.data<-canopy.clos.all%>%
  select(Park, EcoSite, Plot, EventYear, TransectLetter, TreeCanopyClosureCollected,
         CanopyClosurePosition_m, ReadingDirection, CoveredPoints_cnt, 
         CanopyClosure_Reading_pct)

#check number of years
canopy.clos.all.data%>%
  select(EventYear)%>%
  distinct()%>%
  arrange(EventYear)
#check parks and ecosites
canopy.clos.all.data%>%
  select(Park, EcoSite)%>%
  distinct()

#more parks here than we currently use canopy closure for. Remove pilot data
canopy.clos.parks<-canopy.clos.all.data%>%
  filter(EcoSite %in% c("BAND_M", "GRCA_M", "WACA_P"))

#check for blanks or NA's
nacheck<-canopy.clos.parks%>%
  filter_all(any_vars(is.na(.)))

canopy.clos.parks<-canopy.clos.parks%>%
  replace_na(list(CoveredPoints_cnt=-99999, 
                  CanopyClosure_Reading_pct  = -99999, CanopyClosurePosition_m = -99999,
                  ReadingDirection = "NAN"))
#recheck
nacheck<-canopy.clos.parks%>%
  filter_all(any_vars(is.na(.)))

write.csv(canopy.clos.parks, "view_CanopyClosure_All_2007-2024.csv", row.names = F)

dbDisconnect(conUpE)