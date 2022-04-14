####
# This is the script for storing the schema of your TerminusDB
# database for your project.
# Use 'terminusdb commit' to commit changes to the database and
# use 'terminusdb sync' to change this file according to
# the exsisting database schema
####
from typing import Optional, Set

from terminusdb_client.woqlschema import DocumentTemplate, LexicalKey


class GenomeFeature(DocumentTemplate):
    """A feature localized to an interval along a genome

    Attributes
    ----------
    encodes : Optional['GeneProduct']
        The gene product encoded by this feature. Typically this is used for a CDS feature or gene feature which will encode a protein. It can also be used by a nc transcript ot gene feature that encoded a ncRNA
    end : int
        The end of the feature in positive 1-based integer coordinates
    feature_type : Optional[str]
        TODO: Yuri to write
    phase : Optional[int]
        The phase for a coding sequence entity. For example, phase of a CDS as represented in a GFF3 with a value of 0, 1 or 2.
    seqid : str
        The ID of the landmark used to establish the coordinate system for the current feature.
    start : int
        The start of the feature in positive 1-based integer coordinates
    strand : Optional[str]
        The strand on which a feature is located. Has a value of '+' (sense strand or forward strand) or '-' (anti-sense strand or reverse strand).
    type : Optional[str]
        An optional string that specifies the type object.  This is used to allow for searches for different kinds of objects.
    """

    encodes: Optional["GeneProduct"]
    end: int
    feature_type: Optional[str]
    phase: Optional[int]
    seqid: str
    start: int
    strand: Optional[str]
    type: Optional[str]


class AttributeValue(DocumentTemplate):
    """The value for any value of a attribute for a sample. This object can hold both the un-normalized atomic value and the structured value

    Attributes
    ----------
    has_raw_value : Optional[str]
        The value that was specified for an annotation in raw form, i.e. a string. E.g. "2 cm" or "2-4 cm"
    type : Optional[str]
        An optional string that specifies the type object.  This is used to allow for searches for different kinds of objects.
    was_generated_by : Optional['Activity']
        null
    """

    has_raw_value: Optional[str]
    type: Optional[str]
    was_generated_by: Optional["Activity"]


class PersonValue(AttributeValue):
    """An attribute value representing a person

    Attributes
    ----------
    email : Optional[str]
        An email address for an entity such as a person. This should be the primarly email address used.
    has_raw_value : Optional[str]
        The value that was specified for an annotation in raw form, i.e. a string. E.g. "2 cm" or "2-4 cm"
    name : Optional[str]
        A human readable label for an entity
    orcid : Optional[str]
        The ORICD of a person.
    profile_image_url : Optional[str]
        A url that points to an image of a person.
    websites : Set[str]
        A list of websites that are assocatiated with the entity.
    """

    email: Optional[str]
    has_raw_value: Optional[str]
    name: Optional[str]
    orcid: Optional[str]
    profile_image_url: Optional[str]
    websites: Set[str]


class ReactionParticipant(DocumentTemplate):
    """Instances of this link a reaction to a chemical entity participant

    Attributes
    ----------
    chemical : Optional['ChemicalEntity']
        null
    stoichiometry : Optional[int]
        null
    """

    chemical: Optional["ChemicalEntity"]
    stoichiometry: Optional[int]


class NamedThing(DocumentTemplate):
    """a databased entity or concept/class

    Attributes
    ----------
    alternative_identifiers : Set[str]
        A list of alternative identifiers for the entity.
    description : Optional[str]
        a human-readable description of a thing
    id : str
        A unique identifier for a thing. Must be either a CURIE shorthand for a URI or a complete URI
    name : Optional[str]
        A human readable label for an entity
    """

    _key = LexicalKey(["id"])
    _abstract = []
    alternative_identifiers: Set[str]
    description: Optional[str]
    id: str
    name: Optional[str]


class Person(NamedThing):
    """represents a person, such as a researcher

    Attributes
    ----------
    id : str
        A unique identifier for a thing. Must be either a CURIE shorthand for a URI or a complete URI
    """

    _key = LexicalKey(["id"])
    id: str


class Biosample(NamedThing):
    """A material sample. It may be environmental (encompassing many organisms) or isolate or tissue.   An environmental sample containing genetic material from multiple individuals is commonly referred to as a biosample.

    Attributes
    ----------
    GOLD_sample_identifiers : Set['xsd:anyURI']
        identifiers for corresponding sample in GOLD
    INSDC_biosample_identifiers : Set['xsd:anyURI']
        identifiers for corresponding sample in INSDC
    INSDC_secondary_sample_identifiers : Set['xsd:anyURI']
        secondary identifiers for corresponding sample in INSDC
    add_date : Optional[str]
        The date on which the information was added to the database.
    agrochem_addition : Optional['QuantityValue']
        Addition of fertilizers, pesticides, etc. - amount and time of applications
    al_sat : Optional['QuantityValue']
        Aluminum saturation (esp. For tropical soils)
    al_sat_meth : Optional['TextValue']
        Reference or method used in determining Al saturation
    alkalinity : Optional['QuantityValue']
        Alkalinity, the ability of a solution to neutralize acids to the equivalence point of carbonate or bicarbonate
    alkalinity_method : Optional['TextValue']
        Method used for alkalinity measurement
    alkyl_diethers : Optional['QuantityValue']
        Concentration of alkyl diethers
    alt : Optional['QuantityValue']
        Altitude is a term used to identify heights of objects such as airplanes, space shuttles, rockets, atmospheric balloons and heights of places such as atmospheric layers and clouds. It is used to measure the height of an object which is above the earthbs surface. In this context, the altitude measurement is the vertical distance between the earth's surface above sea level and the sampled position in the air
    aminopept_act : Optional['QuantityValue']
        Measurement of aminopeptidase activity
    ammonium : Optional['QuantityValue']
        Concentration of ammonium in the sample
    annual_precpt : Optional['QuantityValue']
        The average of all annual precipitation values known, or an estimated equivalent value derived by such methods as regional indexes or Isohyetal maps.
    annual_temp : Optional['QuantityValue']
        Mean annual temperature
    bacteria_carb_prod : Optional['QuantityValue']
        Measurement of bacterial carbon production
    bishomohopanol : Optional['QuantityValue']
        Concentration of bishomohopanol
    bromide : Optional['QuantityValue']
        Concentration of bromide
    calcium : Optional['QuantityValue']
        Concentration of calcium in the sample
    carb_nitro_ratio : Optional['QuantityValue']
        Ratio of amount or concentrations of carbon to nitrogen
    chem_administration : Optional['ControlledTermValue']
        List of chemical compounds administered to the host or site where sampling occurred, and when (e.g. Antibiotics, n fertilizer, air filter); can include multiple compounds. For chemical entities of biological interest ontology (chebi) (v 163), http://purl.bioontology.org/ontology/chebi
    chloride : Optional['QuantityValue']
        Concentration of chloride in the sample
    chlorophyll : Optional['QuantityValue']
        Concentration of chlorophyll
    collection_date : Optional['TimestampValue']
        The time of sampling, either as an instance (single point in time) or interval. In case no exact time is available, the date/time can be right truncated i.e. all of these are valid times: 2008-01-23T19:23:10+00:00; 2008-01-23T19:23:10; 2008-01-23; 2008-01; 2008; Except: 2008-01; 2008 all are ISO8601 compliant
    community : Optional[str]
        null
    crop_rotation : Optional['TextValue']
        Whether or not crop is rotated, and if yes, rotation schedule
    cur_land_use : Optional['TextValue']
        Present state of sample site
    cur_vegetation : Optional['TextValue']
        Vegetation classification from one or more standard classification systems, or agricultural crop
    cur_vegetation_meth : Optional['TextValue']
        Reference or method used in vegetation classification
    density : Optional['QuantityValue']
        Density of the sample, which is its mass per unit volume (aka volumetric mass density)
    depth : Optional['QuantityValue']
        Depth is defined as the vertical distance below local surface, e.g. For sediment or soil samples depth is measured from sediment or soil surface, respectively. Depth can be reported as an interval for subsurface samples
    depth2 : Optional['QuantityValue']
        null
    diss_carb_dioxide : Optional['QuantityValue']
        Concentration of dissolved carbon dioxide in the sample or liquid portion of the sample
    diss_hydrogen : Optional['QuantityValue']
        Concentration of dissolved hydrogen
    diss_inorg_carb : Optional['QuantityValue']
        Dissolved inorganic carbon concentration in the sample, typically measured after filtering the sample using a 0.45 micrometer filter
    diss_inorg_phosp : Optional['QuantityValue']
        Concentration of dissolved inorganic phosphorus in the sample
    diss_org_carb : Optional['QuantityValue']
        Concentration of dissolved organic carbon in the sample, liquid portion of the sample, or aqueous phase of the fluid
    diss_org_nitro : Optional['QuantityValue']
        Dissolved organic nitrogen concentration measured as; total dissolved nitrogen - NH4 - NO3 - NO2
    diss_oxygen : Optional['QuantityValue']
        Concentration of dissolved oxygen
    drainage_class : Optional['TextValue']
        Drainage classification from a standard system such as the USDA system
    ecosystem : Optional[str]
        An ecosystem is a combination of a physical environment (abiotic factors) and all the organisms (biotic factors) that interact with this environment. Ecosystem is in position 1/5 in a GOLD path.
    ecosystem_category : Optional[str]
        Ecosystem categories represent divisions within the ecosystem based on specific characteristics of the environment from where an organism or sample is isolated. Ecosystem category is in position 2/5 in a GOLD path.
    ecosystem_subtype : Optional[str]
        Ecosystem subtypes represent further subdivision of Ecosystem types into more distinct subtypes. Ecosystem subtype is in position 4/5 in a GOLD path.
    ecosystem_type : Optional[str]
        Ecosystem types represent things having common characteristics within the Ecosystem Category. These common characteristics based grouping is still broad but specific to the characteristics of a given environment. Ecosystem type is in position 3/5 in a GOLD path.
    elev : Optional['QuantityValue']
        Elevation of the sampling site is its height above a fixed reference point, most commonly the mean sea level. Elevation is mainly used when referring to points on the earth's surface, while altitude is used for points above the surface, such as an aircraft in flight or a spacecraft in orbit
    env_broad_scale : Optional['ControlledTermValue']
        In this field, report which major environmental system your sample or specimen came from. The systems identified should have a coarse spatial grain, to provide the general environmental context of where the sampling was done (e.g. were you in the desert or a rainforest?). We recommend using subclasses of ENVOUs biome class: http://purl.obolibrary.org/obo/ENVO_00000428. Format (one term): termLabel [termID], Format (multiple terms): termLabel [termID]|termLabel [termID]|termLabel [termID]. Example: Annotating a water sample from the photic zone in middle of the Atlantic Ocean, consider: oceanic epipelagic zone biome [ENVO:01000033]. Example: Annotating a sample from the Amazon rainforest consider: tropical moist broadleaf forest biome [ENVO:01000228]. If needed, request new terms on the ENVO tracker, identified here: http://www.obofoundry.org/ontology/envo.html
    env_local_scale : Optional['ControlledTermValue']
        In this field, report the entity or entities which are in your sample or specimenUs local vicinity and which you believe have significant causal influences on your sample or specimen. Please use terms that are present in ENVO and which are of smaller spatial grain than your entry for env_broad_scale. Format (one term): termLabel [termID]; Format (multiple terms): termLabel [termID]|termLabel [termID]|termLabel [termID]. Example: Annotating a pooled sample taken from various vegetation layers in a forest consider: canopy [ENVO:00000047]|herb and fern layer [ENVO:01000337]|litter layer [ENVO:01000338]|understory [01000335]|shrub layer [ENVO:01000336]. If needed, request new terms on the ENVO tracker, identified here: http://www.obofoundry.org/ontology/envo.html
    env_medium : Optional['ControlledTermValue']
        In this field, report which environmental material or materials (pipe separated) immediately surrounded your sample or specimen prior to sampling, using one or more subclasses of ENVOUs environmental material class: http://purl.obolibrary.org/obo/ENVO_00010483. Format (one term): termLabel [termID]; Format (multiple terms): termLabel [termID]|termLabel [termID]|termLabel [termID]. Example: Annotating a fish swimming in the upper 100 m of the Atlantic Ocean, consider: ocean water [ENVO:00002151]. Example: Annotating a duck on a pond consider: pond water [ENVO:00002228]|air ENVO_00002005. If needed, request new terms on the ENVO tracker, identified here: http://www.obofoundry.org/ontology/envo.html
    env_package : Optional['TextValue']
        MIxS extension for reporting of measurements and observations obtained from one or more of the environments where the sample was obtained. All environmental packages listed here are further defined in separate subtables. By giving the name of the environmental package, a selection of fields can be made from the subtables and can be reported
    extreme_event : Optional['TimestampValue']
        Unusual physical events that may have affected microbial populations
    fao_class : Optional['TextValue']
        Soil classification from the FAO World Reference Database for Soil Resources. The list can be found at http://www.fao.org/nr/land/sols/soil/wrb-soil-maps/reference-groups
    fire : Optional['TimestampValue']
        Historical and/or physical evidence of fire
    flooding : Optional['TimestampValue']
        Historical and/or physical evidence of flooding
    geo_loc_name : Optional['TextValue']
        The geographical origin of the sample as defined by the country or sea name followed by specific region name. Country or sea names should be chosen from the INSDC country list (http://insdc.org/country.html), or the GAZ ontology (v 1.512) (http://purl.bioontology.org/ontology/GAZ)
    glucosidase_act : Optional['QuantityValue']
        Measurement of glucosidase activity
    habitat : Optional[str]
        null
    heavy_metals : Optional['QuantityValue']
        Heavy metals present and concentrationsany drug used by subject and the frequency of usage; can include multiple heavy metals and concentrations
    heavy_metals_meth : Optional['TextValue']
        Reference or method used in determining heavy metals
    horizon : Optional['TextValue']
        Specific layer in the land area which measures parallel to the soil surface and possesses physical characteristics which differ from the layers above and beneath
    horizon_meth : Optional['TextValue']
        Reference or method used in determining the horizon
    host_name : Optional[str]
        null
    identifier : Optional[str]
        null
    lat_lon : Optional['GeolocationValue']
        The geographical origin of the sample as defined by latitude and longitude. The values should be reported in decimal degrees and in WGS84 system
    link_addit_analys : Optional['TextValue']
        Link to additional analysis results performed on the sample
    link_class_info : Optional['TextValue']
        Link to digitized soil maps or other soil classification information
    link_climate_info : Optional['TextValue']
        Link to climate resource
    local_class : Optional['TextValue']
        Soil classification based on local soil classification system
    local_class_meth : Optional['TextValue']
        Reference or method used in determining the local soil classification
    location : Optional[str]
        null
    magnesium : Optional['QuantityValue']
        Concentration of magnesium in the sample
    mean_frict_vel : Optional['QuantityValue']
        Measurement of mean friction velocity
    mean_peak_frict_vel : Optional['QuantityValue']
        Measurement of mean peak friction velocity
    microbial_biomass : Optional['QuantityValue']
        The part of the organic matter in the soil that constitutes living microorganisms smaller than 5-10 micrometer. If you keep this, you would need to have correction factors used for conversion to the final units
    microbial_biomass_meth : Optional['TextValue']
        Reference or method used in determining microbial biomass
    misc_param : Optional['QuantityValue']
        Any other measurement performed or parameter collected, that is not listed here
    mod_date : Optional[str]
        The last date on which the database information was modified.
    n_alkanes : Optional['QuantityValue']
        Concentration of n-alkanes; can include multiple n-alkanes
    ncbi_taxonomy_name : Optional[str]
        null
    nitrate : Optional['QuantityValue']
        Concentration of nitrate in the sample
    nitrite : Optional['QuantityValue']
        Concentration of nitrite in the sample
    org_matter : Optional['QuantityValue']
        Concentration of organic matter
    org_nitro : Optional['QuantityValue']
        Concentration of organic nitrogen
    organism_count : Optional['QuantityValue']
        Total cell count of any organism (or group of organisms) per gram, volume or area of sample, should include name of organism followed by count. The method that was used for the enumeration (e.g. qPCR, atp, mpn, etc.) Should also be provided. (example: total prokaryotes; 3.5e7 cells per ml; qpcr)
    oxy_stat_samp : Optional['TextValue']
        Oxygenation status of sample
    part_of : Set['NamedThing']
        Links a resource to another resource that either logically or physically includes it.
    part_org_carb : Optional['QuantityValue']
        Concentration of particulate organic carbon
    perturbation : Optional['TextValue']
        Type of perturbation, e.g. chemical administration, physical disturbance, etc., coupled with perturbation regimen including how many times the perturbation was repeated, how long each perturbation lasted, and the start and end time of the entire perturbation period; can include multiple perturbation types
    petroleum_hydrocarb : Optional['QuantityValue']
        Concentration of petroleum hydrocarbon
    ph : Optional['QuantityValue']
        Ph measurement of the sample, or liquid portion of sample, or aqueous phase of the fluid
    ph_meth : Optional['TextValue']
        Reference or method used in determining ph
    phaeopigments : Optional['QuantityValue']
        Concentration of phaeopigments; can include multiple phaeopigments
    phosplipid_fatt_acid : Optional['QuantityValue']
        Concentration of phospholipid fatty acids; can include multiple values
    pool_dna_extracts : Optional['TextValue']
        Indicate whether multiple DNA extractions were mixed. If the answer yes, the number of extracts that were pooled should be given
    potassium : Optional['QuantityValue']
        Concentration of potassium in the sample
    pressure : Optional['QuantityValue']
        Pressure to which the sample is subject to, in atmospheres
    previous_land_use : Optional['TextValue']
        Previous land use and dates
    previous_land_use_meth : Optional['TextValue']
        Reference or method used in determining previous land use and dates
    profile_position : Optional['TextValue']
        Cross-sectional position in the hillslope where sample was collected.sample area position in relation to surrounding areas
    proport_woa_temperature : Optional[str]
        null
    redox_potential : Optional['QuantityValue']
        Redox potential, measured relative to a hydrogen cell, indicating oxidation or reduction potential
    salinity : Optional['QuantityValue']
        Salinity is the total concentration of all dissolved salts in a water sample. While salinity can be measured by a complete chemical analysis, this method is difficult and time consuming. More often, it is instead derived from the conductivity measurement. This is known as practical salinity. These derivations compare the specific conductance of the sample to a salinity standard such as seawater
    salinity_category : Optional[str]
        Categorcial description of the sample's salinity. Examples: halophile, halotolerant, hypersaline, huryhaline
    salinity_meth : Optional['TextValue']
        Reference or method used in determining salinity
    samp_collect_device : Optional['TextValue']
        The method or device employed for collecting the sample
    samp_mat_process : Optional['ControlledTermValue']
        Any processing applied to the sample during or after retrieving the sample from environment. This field accepts OBI, for a browser of OBI (v 2018-02-12) terms please see http://purl.bioontology.org/ontology/OBI
    samp_store_dur : Optional['TextValue']
        Duration for which the sample was stored
    samp_store_loc : Optional['TextValue']
        Location at which sample was stored, usually name of a specific freezer/room
    samp_store_temp : Optional['QuantityValue']
        Temperature at which sample was stored, e.g. -80 degree Celsius
    samp_vol_we_dna_ext : Optional['QuantityValue']
        Volume (ml), weight (g) of processed sample, or surface area swabbed from sample for DNA extraction
    sample_collection_site : Optional[str]
        null
    season_precpt : Optional['QuantityValue']
        The average of all seasonal precipitation values known, or an estimated equivalent value derived by such methods as regional indexes or Isohyetal maps.
    season_temp : Optional['QuantityValue']
        Mean seasonal temperature
    sieving : Optional['QuantityValue']
        Collection design of pooled samples and/or sieve size and amount of sample sieved
    size_frac_low : Optional['QuantityValue']
        Refers to the mesh/pore size used to pre-filter/pre-sort the sample. Materials larger than the size threshold are excluded from the sample
    size_frac_up : Optional['QuantityValue']
        Refers to the mesh/pore size used to retain the sample. Materials smaller than the size threshold are excluded from the sample
    slope_aspect : Optional['QuantityValue']
        The direction a slope faces. While looking down a slope use a compass to record the direction you are facing (direction or degrees); e.g., nw or 315 degrees. This measure provides an indication of sun and wind exposure that will influence soil temperature and evapotranspiration.
    slope_gradient : Optional['QuantityValue']
        Commonly called 'slope'. The angle between ground surface and a horizontal line (in percent). This is the direction that overland water would flow. This measure is usually taken with a hand level meter or clinometer
    sodium : Optional['QuantityValue']
        Sodium concentration in the sample
    soil_type : Optional['TextValue']
        Soil series name or other lower-level classification
    soil_type_meth : Optional['TextValue']
        Reference or method used in determining soil series name or other lower-level classification
    soluble_iron_micromol : Optional[str]
        null
    specific_ecosystem : Optional[str]
        Specific ecosystems represent specific features of the environment like aphotic zone in an ocean or gastric mucosa within a host digestive system. Specific ecosystem is in position 5/5 in a GOLD path.
    store_cond : Optional['TextValue']
        Explain how and for how long the soil sample was stored before DNA extraction
    subsurface_depth : Optional['QuantityValue']
        null
    subsurface_depth2 : Optional['QuantityValue']
        null
    sulfate : Optional['QuantityValue']
        Concentration of sulfate in the sample
    sulfide : Optional['QuantityValue']
        Concentration of sulfide in the sample
    temp : Optional['QuantityValue']
        Temperature of the sample at the time of sampling
    texture : Optional['QuantityValue']
        The relative proportion of different grain sizes of mineral particles in a soil, as described using a standard system; express as % sand (50 um to 2 mm), silt (2 um to 50 um), and clay (<2 um) with textural name (e.g., silty clay loam) optional.
    texture_meth : Optional['TextValue']
        Reference or method used in determining soil texture
    tidal_stage : Optional['TextValue']
        Stage of tide
    tillage : Optional['TextValue']
        Note method(s) used for tilling
    tot_carb : Optional['QuantityValue']
        Total carbon content
    tot_depth_water_col : Optional['QuantityValue']
        Measurement of total depth of water column
    tot_diss_nitro : Optional['QuantityValue']
        Total dissolved nitrogen concentration, reported as nitrogen, measured by: total dissolved nitrogen = NH4 + NO3NO2 + dissolved organic nitrogen
    tot_nitro_content : Optional['QuantityValue']
        Total nitrogen content of the sample
    tot_nitro_content_meth : Optional['TextValue']
        Reference or method used in determining the total nitrogen
    tot_org_c_meth : Optional['TextValue']
        Reference or method used in determining total organic carbon
    tot_org_carb : Optional['QuantityValue']
        Definition for soil: total organic carbon content of the soil, definition otherwise: total organic carbon content
    tot_phosp : Optional['QuantityValue']
        Total phosphorus concentration in the sample, calculated by: total phosphorus = total dissolved phosphorus + particulate phosphorus
    type : Optional[str]
        An optional string that specifies the type object.  This is used to allow for searches for different kinds of objects.
    water_content : Optional['QuantityValue']
        Water content measurement
    water_content_soil_meth : Optional['TextValue']
        Reference or method used in determining the water content of soil
    """

    _key = LexicalKey(["id"])
    GOLD_sample_identifiers: Set["xsd:anyURI"]
    INSDC_biosample_identifiers: Set["xsd:anyURI"]
    INSDC_secondary_sample_identifiers: Set["xsd:anyURI"]
    add_date: Optional[str]
    agrochem_addition: Optional["QuantityValue"]
    al_sat: Optional["QuantityValue"]
    al_sat_meth: Optional["TextValue"]
    alkalinity: Optional["QuantityValue"]
    alkalinity_method: Optional["TextValue"]
    alkyl_diethers: Optional["QuantityValue"]
    alt: Optional["QuantityValue"]
    aminopept_act: Optional["QuantityValue"]
    ammonium: Optional["QuantityValue"]
    annual_precpt: Optional["QuantityValue"]
    annual_temp: Optional["QuantityValue"]
    bacteria_carb_prod: Optional["QuantityValue"]
    bishomohopanol: Optional["QuantityValue"]
    bromide: Optional["QuantityValue"]
    calcium: Optional["QuantityValue"]
    carb_nitro_ratio: Optional["QuantityValue"]
    chem_administration: Optional["ControlledTermValue"]
    chloride: Optional["QuantityValue"]
    chlorophyll: Optional["QuantityValue"]
    collection_date: Optional["TimestampValue"]
    community: Optional[str]
    crop_rotation: Optional["TextValue"]
    cur_land_use: Optional["TextValue"]
    cur_vegetation: Optional["TextValue"]
    cur_vegetation_meth: Optional["TextValue"]
    density: Optional["QuantityValue"]
    depth: Optional["QuantityValue"]
    depth2: Optional["QuantityValue"]
    diss_carb_dioxide: Optional["QuantityValue"]
    diss_hydrogen: Optional["QuantityValue"]
    diss_inorg_carb: Optional["QuantityValue"]
    diss_inorg_phosp: Optional["QuantityValue"]
    diss_org_carb: Optional["QuantityValue"]
    diss_org_nitro: Optional["QuantityValue"]
    diss_oxygen: Optional["QuantityValue"]
    drainage_class: Optional["TextValue"]
    ecosystem: Optional[str]
    ecosystem_category: Optional[str]
    ecosystem_subtype: Optional[str]
    ecosystem_type: Optional[str]
    elev: Optional["QuantityValue"]
    env_broad_scale: Optional["ControlledTermValue"]
    env_local_scale: Optional["ControlledTermValue"]
    env_medium: Optional["ControlledTermValue"]
    env_package: Optional["TextValue"]
    extreme_event: Optional["TimestampValue"]
    fao_class: Optional["TextValue"]
    fire: Optional["TimestampValue"]
    flooding: Optional["TimestampValue"]
    geo_loc_name: Optional["TextValue"]
    glucosidase_act: Optional["QuantityValue"]
    habitat: Optional[str]
    heavy_metals: Optional["QuantityValue"]
    heavy_metals_meth: Optional["TextValue"]
    horizon: Optional["TextValue"]
    horizon_meth: Optional["TextValue"]
    host_name: Optional[str]
    identifier: Optional[str]
    lat_lon: Optional["GeolocationValue"]
    link_addit_analys: Optional["TextValue"]
    link_class_info: Optional["TextValue"]
    link_climate_info: Optional["TextValue"]
    local_class: Optional["TextValue"]
    local_class_meth: Optional["TextValue"]
    location: Optional[str]
    magnesium: Optional["QuantityValue"]
    mean_frict_vel: Optional["QuantityValue"]
    mean_peak_frict_vel: Optional["QuantityValue"]
    microbial_biomass: Optional["QuantityValue"]
    microbial_biomass_meth: Optional["TextValue"]
    misc_param: Optional["QuantityValue"]
    mod_date: Optional[str]
    n_alkanes: Optional["QuantityValue"]
    ncbi_taxonomy_name: Optional[str]
    nitrate: Optional["QuantityValue"]
    nitrite: Optional["QuantityValue"]
    org_matter: Optional["QuantityValue"]
    org_nitro: Optional["QuantityValue"]
    organism_count: Optional["QuantityValue"]
    oxy_stat_samp: Optional["TextValue"]
    part_of: Set["NamedThing"]
    part_org_carb: Optional["QuantityValue"]
    perturbation: Optional["TextValue"]
    petroleum_hydrocarb: Optional["QuantityValue"]
    ph: Optional["QuantityValue"]
    ph_meth: Optional["TextValue"]
    phaeopigments: Optional["QuantityValue"]
    phosplipid_fatt_acid: Optional["QuantityValue"]
    pool_dna_extracts: Optional["TextValue"]
    potassium: Optional["QuantityValue"]
    pressure: Optional["QuantityValue"]
    previous_land_use: Optional["TextValue"]
    previous_land_use_meth: Optional["TextValue"]
    profile_position: Optional["TextValue"]
    proport_woa_temperature: Optional[str]
    redox_potential: Optional["QuantityValue"]
    salinity: Optional["QuantityValue"]
    salinity_category: Optional[str]
    salinity_meth: Optional["TextValue"]
    samp_collect_device: Optional["TextValue"]
    samp_mat_process: Optional["ControlledTermValue"]
    samp_store_dur: Optional["TextValue"]
    samp_store_loc: Optional["TextValue"]
    samp_store_temp: Optional["QuantityValue"]
    samp_vol_we_dna_ext: Optional["QuantityValue"]
    sample_collection_site: Optional[str]
    season_precpt: Optional["QuantityValue"]
    season_temp: Optional["QuantityValue"]
    sieving: Optional["QuantityValue"]
    size_frac_low: Optional["QuantityValue"]
    size_frac_up: Optional["QuantityValue"]
    slope_aspect: Optional["QuantityValue"]
    slope_gradient: Optional["QuantityValue"]
    sodium: Optional["QuantityValue"]
    soil_type: Optional["TextValue"]
    soil_type_meth: Optional["TextValue"]
    soluble_iron_micromol: Optional[str]
    specific_ecosystem: Optional[str]
    store_cond: Optional["TextValue"]
    subsurface_depth: Optional["QuantityValue"]
    subsurface_depth2: Optional["QuantityValue"]
    sulfate: Optional["QuantityValue"]
    sulfide: Optional["QuantityValue"]
    temp: Optional["QuantityValue"]
    texture: Optional["QuantityValue"]
    texture_meth: Optional["TextValue"]
    tidal_stage: Optional["TextValue"]
    tillage: Optional["TextValue"]
    tot_carb: Optional["QuantityValue"]
    tot_depth_water_col: Optional["QuantityValue"]
    tot_diss_nitro: Optional["QuantityValue"]
    tot_nitro_content: Optional["QuantityValue"]
    tot_nitro_content_meth: Optional["TextValue"]
    tot_org_c_meth: Optional["TextValue"]
    tot_org_carb: Optional["QuantityValue"]
    tot_phosp: Optional["QuantityValue"]
    type: Optional[str]
    water_content: Optional["QuantityValue"]
    water_content_soil_meth: Optional["TextValue"]


class Database(DocumentTemplate):
    """An abstract holder for any set of metadata and data. It does not need to correspond to an actual managed databse top level holder class. When translated to JSON-Schema this is the 'root' object. It should contain pointers to other objects of interest

    Attributes
    ----------
    activity_set : Set['WorkflowExecutionActivity']
        This property links a database object to the set of workflow activities.
    biosample_set : Set['Biosample']
        This property links a database object to the set of samples within it.
    data_object_set : Set['DataObject']
        This property links a database object to the set of data objects within it.
    date_created : Optional[str]
        TODO
    etl_software_version : Optional[str]
        TODO
    functional_annotation_set : Set['FunctionalAnnotation']
        This property links a database object to the set of all functional annotations
    genome_feature_set : Set['GenomeFeature']
        This property links a database object to the set of all features
    mags_activity_set : Set['MAGsAnalysisActivity']
        This property links a database object to the set of MAGs analysis activities.
    metabolomics_analysis_activity_set : Set['MetabolomicsAnalysisActivity']
        This property links a database object to the set of metabolomics analysis activities.
    metagenome_annotation_activity_set : Set['MetagenomeAnnotationActivity']
        This property links a database object to the set of metagenome annotation activities.
    metagenome_assembly_set : Set['MetagenomeAssembly']
        This property links a database object to the set of metagenome assembly activities.
    metaproteomics_analysis_activity_set : Set['MetaproteomicsAnalysisActivity']
        This property links a database object to the set of metaproteomics analysis activities.
    metatranscriptome_activity_set : Set['MetatranscriptomeActivity']
        This property links a database object to the set of metatranscriptome analysis activities.
    nmdc_schema_version : Optional[str]
        TODO
    nom_analysis_activity_set : Set['NomAnalysisActivity']
        This property links a database object to the set of natural organic matter (NOM) analysis activities.
    omics_processing_set : Set['OmicsProcessing']
        This property links a database object to the set of omics processings within it.
    read_QC_analysis_activity_set : Set['ReadQCAnalysisActivity']
        This property links a database object to the set of read QC analysis activities.
    read_based_analysis_activity_set : Set['ReadBasedAnalysisActivity']
        This property links a database object to the set of read based analysis activities.

    study_set : Set['Study']
        This property links a database object to the set of studies within it.
    """

    activity_set: Set["WorkflowExecutionActivity"]
    biosample_set: Set["Biosample"]
    data_object_set: Set["DataObject"]
    date_created: Optional[str]
    etl_software_version: Optional[str]
    functional_annotation_set: Set["FunctionalAnnotation"]
    genome_feature_set: Set["GenomeFeature"]
    mags_activity_set: Set["MAGsAnalysisActivity"]
    metabolomics_analysis_activity_set: Set["MetabolomicsAnalysisActivity"]
    metagenome_annotation_activity_set: Set["MetagenomeAnnotationActivity"]
    metagenome_assembly_set: Set["MetagenomeAssembly"]
    metaproteomics_analysis_activity_set: Set["MetaproteomicsAnalysisActivity"]
    metatranscriptome_activity_set: Set["MetatranscriptomeActivity"]
    nmdc_schema_version: Optional[str]
    nom_analysis_activity_set: Set["NomAnalysisActivity"]
    omics_processing_set: Set["OmicsProcessing"]
    read_QC_analysis_activity_set: Set["ReadQCAnalysisActivity"]
    read_based_analysis_activity_set: Set["ReadBasedAnalysisActivity"]
    study_set: Set["Study"]


class FunctionalAnnotation(DocumentTemplate):
    """An assignment of a function term (e.g. reaction or pathway) that is executed by a gene product, or which the gene product plays an active role in. Functional annotations can be assigned manually by curators, or automatically in workflows. In the context of NMDC, all function annotation is performed automatically, typically using HMM or Blast type methods

    Attributes
    ----------
    has_function : Optional[str]
        null
    subject : Optional['GeneProduct']
        null
    type : Optional[str]
        An optional string that specifies the type object.  This is used to allow for searches for different kinds of objects.
    was_generated_by : Optional['Activity']
        null
    """

    has_function: Optional[str]
    subject: Optional["GeneProduct"]
    type: Optional[str]
    was_generated_by: Optional["Activity"]


class BiosampleProcessing(NamedThing):
    """A process that takes one or more biosamples as inputs and generates one or as outputs. Examples of outputs include samples cultivated from another sample or data objects created by instruments runs.

    Attributes
    ----------
    has_input : Set['NamedThing']
        An input to a process.
    """

    _key = LexicalKey(["id"])
    has_input: Set["NamedThing"]


class MAGBin(DocumentTemplate):
    """

    Attributes
    ----------
    bin_name : Optional[str]
        null
    bin_quality : Optional[str]
        null
    completeness : Optional['xsd:float']
        null
    contamination : Optional['xsd:float']
        null
    gene_count : Optional[int]
        null
    gtdbtk_class : Optional[str]
        null
    gtdbtk_domain : Optional[str]
        null
    gtdbtk_family : Optional[str]
        null
    gtdbtk_genus : Optional[str]
        null
    gtdbtk_order : Optional[str]
        null
    gtdbtk_phylum : Optional[str]
        null
    gtdbtk_species : Optional[str]
        null
    num_16s : Optional[int]
        null
    num_23s : Optional[int]
        null
    num_5s : Optional[int]
        null
    num_tRNA : Optional[int]
        null
    number_of_contig : Optional[int]
        null
    type : Optional[str]
        An optional string that specifies the type object.  This is used to allow for searches for different kinds of objects.
    """

    bin_name: Optional[str]
    bin_quality: Optional[str]
    completeness: Optional["xsd:float"]
    contamination: Optional["xsd:float"]
    gene_count: Optional[int]
    gtdbtk_class: Optional[str]
    gtdbtk_domain: Optional[str]
    gtdbtk_family: Optional[str]
    gtdbtk_genus: Optional[str]
    gtdbtk_order: Optional[str]
    gtdbtk_phylum: Optional[str]
    gtdbtk_species: Optional[str]
    num_16s: Optional[int]
    num_23s: Optional[int]
    num_5s: Optional[int]
    num_tRNA: Optional[int]
    number_of_contig: Optional[int]
    type: Optional[str]


class GeneProduct(NamedThing):
    """A molecule encoded by a gene that has an evolved function"""

    _key = LexicalKey(["id"])


class Instrument(NamedThing):
    """A material entity that is designed to perform a function in a scientific investigation, but is not a reagent[OBI]."""

    _key = LexicalKey(["id"])


class OntologyClass(NamedThing):
    """"""

    _key = LexicalKey(["id"])


class ChemicalEntity(OntologyClass):
    """An atom or molecule that can be represented with a chemical formula. Include lipids, glycans, natural products, drugs. There may be different terms for distinct acid-base forms, protonation states

    Attributes
    ----------
    chemical_formula : Optional[str]
        A generic grouping for miolecular formulae and empirican formulae
    inchi : Optional[str]
        null
    inchi_key : Optional[str]
        null
    smiles : Set[str]
        A string encoding of a molecular graph, no chiral or isotopic information. There are usually a large number of valid SMILES which represent a given structure. For example, CCO, OCC and C(O)C all specify the structure of ethanol.
    """

    _key = LexicalKey(["id"])
    chemical_formula: Optional[str]
    inchi: Optional[str]
    inchi_key: Optional[str]
    smiles: Set[str]


class Study(NamedThing):
    """A study summarizes the overall goal of a research initiative and outlines the key objective of its underlying projects.

    Attributes
    ----------
    GOLD_study_identifiers : Set['xsd:anyURI']
        identifiers for corresponding project in GOLD
    INSDC_SRA_ENA_study_identifiers : Set['xsd:anyURI']
        identifiers for corresponding project in INSDC SRA / ENA
    INSDC_bioproject_identifiers : Set['xsd:anyURI']
        identifiers for corresponding project in INSDC Bioproject
    MGnify_project_identifiers : Set['xsd:anyURI']
        identifiers for corresponding project in MGnify
    abstract : Optional[str]
        The abstract of manuscript/grant associated with the entity; i.e., a summary of the resource.
    alternative_descriptions : Set[str]
        A list of alternative descriptions for the entity. The distinction between desciption and alternative descriptions is application-specific.
    alternative_names : Set[str]
        A list of alternative names used to refer to the entity. The distinction between name and alternative names is application-specific.
    alternative_titles : Set[str]
        A list of alternative titles for the entity. The distinction between title and alternative titles is application-specific.
    doi : Optional['AttributeValue']
        null
    ecosystem : Optional[str]
        An ecosystem is a combination of a physical environment (abiotic factors) and all the organisms (biotic factors) that interact with this environment. Ecosystem is in position 1/5 in a GOLD path.
    ecosystem_category : Optional[str]
        Ecosystem categories represent divisions within the ecosystem based on specific characteristics of the environment from where an organism or sample is isolated. Ecosystem category is in position 2/5 in a GOLD path.
    ecosystem_subtype : Optional[str]
        Ecosystem subtypes represent further subdivision of Ecosystem types into more distinct subtypes. Ecosystem subtype is in position 4/5 in a GOLD path.
    ecosystem_type : Optional[str]
        Ecosystem types represent things having common characteristics within the Ecosystem Category. These common characteristics based grouping is still broad but specific to the characteristics of a given environment. Ecosystem type is in position 3/5 in a GOLD path.
    ess_dive_datasets : Set[str]
        List of ESS-DIVE dataset DOIs
    funding_sources : Set[str]
        null
    has_credit_associations : Set['CreditAssociation']
        This slot links a study to a credit association.  The credit association will be linked to a person value and to a CRediT Contributor Roles term. Overall semantics: person should get credit X for their participation in the study
    objective : Optional[str]
        The scientific objectives associated with the entity. It SHOULD correspond to scientific norms for objectives field in a structured abstract.
    principal_investigator : Optional['PersonValue']
        Principal Investigator who led the study and/or generated the dataset.
    publications : Set[str]
        A list of publications that are assocatiated with the entity. The publicatons SHOULD be given using an identifier, such as a DOI or Pubmed ID, if possible.
    relevant_protocols : Set[str]
        null
    specific_ecosystem : Optional[str]
        Specific ecosystems represent specific features of the environment like aphotic zone in an ocean or gastric mucosa within a host digestive system. Specific ecosystem is in position 5/5 in a GOLD path.
    study_image : Set['ImageValue']
        Links a study to one or more images.
    title : Optional[str]
        A name given to the entity that differs from the name/label programatically assigned to it. For example, when extracting study information for GOLD, the GOLD system has assigned a name/label. However, for display purposes, we may also wish the capture the title of the proposal that was used to fund the study.
    type : Optional[str]
        An optional string that specifies the type object.  This is used to allow for searches for different kinds of objects.
    websites : Set[str]
        A list of websites that are assocatiated with the entity.
    """

    _key = LexicalKey(["id"])
    GOLD_study_identifiers: Set["xsd:anyURI"]
    INSDC_SRA_ENA_study_identifiers: Set["xsd:anyURI"]
    INSDC_bioproject_identifiers: Set["xsd:anyURI"]
    MGnify_project_identifiers: Set["xsd:anyURI"]
    abstract: Optional[str]
    alternative_descriptions: Set[str]
    alternative_names: Set[str]
    alternative_titles: Set[str]
    doi: Optional["AttributeValue"]
    ecosystem: Optional[str]
    ecosystem_category: Optional[str]
    ecosystem_subtype: Optional[str]
    ecosystem_type: Optional[str]
    ess_dive_datasets: Set[str]
    funding_sources: Set[str]
    has_credit_associations: Set["CreditAssociation"]
    objective: Optional[str]
    principal_investigator: Optional["PersonValue"]
    publications: Set[str]
    relevant_protocols: Set[str]
    specific_ecosystem: Optional[str]
    study_image: Set["ImageValue"]
    title: Optional[str]
    type: Optional[str]
    websites: Set[str]


class IntegerValue(AttributeValue):
    """A value that is an integer

    Attributes
    ----------
    has_numeric_value : Optional['xsd:float']
        Links a quantity value to a number
    """

    has_numeric_value: Optional["xsd:float"]


class Activity(DocumentTemplate):
    """a provence-generating activity

    Attributes
    ----------
    ended_at_time : Optional[datetime]
        null
    id : str
        A unique identifier for a thing. Must be either a CURIE shorthand for a URI or a complete URI
    name : Optional[str]
        A human readable label for an entity
    started_at_time : Optional[datetime]
        null
    used : Optional[str]
        null
    was_associated_with : Optional['Agent']
        null
    was_informed_by : Optional['Activity']
        null
    """

    _key = LexicalKey(["id"])
    ended_at_time: Optional[datetime]
    id: str
    name: Optional[str]
    started_at_time: Optional[datetime]
    used: Optional[str]
    was_associated_with: Optional["Agent"]
    was_informed_by: Optional["Activity"]


class BooleanValue(AttributeValue):
    """A value that is a boolean

    Attributes
    ----------
    has_boolean_value : Optional[bool]
        Links a quantity value to a boolean
    """

    has_boolean_value: Optional[bool]


class ControlledTermValue(AttributeValue):
    """A controlled term or class from an ontology

    Attributes
    ----------
    term : Optional['OntologyClass']
        pointer to an ontology class
    """

    term: Optional["OntologyClass"]


class EnvironmentalMaterialTerm(OntologyClass):
    """"""

    _key = LexicalKey(["id"])


class QuantityValue(AttributeValue):
    """A simple quantity, e.g. 2cm

    Attributes
    ----------
    has_maximum_numeric_value : Optional['xsd:float']
        The maximum value part, expressed as number, of the quantity value when the value covers a range.
    has_minimum_numeric_value : Optional['xsd:float']
        The minimum value part, expressed as number, of the quantity value when the value covers a range.
    has_numeric_value : Optional['xsd:float']
        Links a quantity value to a number
    has_raw_value : Optional[str]
        The value that was specified for an annotation in raw form, i.e. a string. E.g. "2 cm" or "2-4 cm"
    has_unit : Optional[str]
        Links a quantity value to a unit
    """

    has_maximum_numeric_value: Optional["xsd:float"]
    has_minimum_numeric_value: Optional["xsd:float"]
    has_numeric_value: Optional["xsd:float"]
    has_raw_value: Optional[str]
    has_unit: Optional[str]


class Agent(DocumentTemplate):
    """a provence-generating agent

    Attributes
    ----------
    acted_on_behalf_of : Optional['Agent']
        null
    was_informed_by : Optional['Activity']
        null
    """

    acted_on_behalf_of: Optional["Agent"]
    was_informed_by: Optional["Activity"]


class GeolocationValue(AttributeValue):
    """A normalized value for a location on the earth's surface

    Attributes
    ----------
    has_raw_value : Optional[str]
        The value that was specified for an annotation in raw form, i.e. a string. E.g. "2 cm" or "2-4 cm"
    latitude : Optional[float]
        latitude
    longitude : Optional[float]
        longitude
    """

    has_raw_value: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]


class CreditAssociation(DocumentTemplate):
    """This class supports binding associated researchers to studies. There will be at least a slot for a CRediT Contributor Role (https://casrai.org/credit/) and for a person value Specifically see the associated researchers tab on the NMDC_SampleMetadata-V4_CommentsForUpdates at https://docs.google.com/spreadsheets/d/1INlBo5eoqn2efn4H2P2i8rwRBtnbDVTqXrochJEAPko/edit#gid=0

    Attributes
    ----------
    applied_role : Optional[str]
        null
    applied_roles : Set[str]
        null
    applies_to_person : 'PersonValue'
        null
    type : Optional[str]
        An optional string that specifies the type object.  This is used to allow for searches for different kinds of objects.
    """

    applied_role: Optional[str]
    applied_roles: Set[str]
    applies_to_person: "PersonValue"
    type: Optional[str]


class PeptideQuantification(DocumentTemplate):
    """This is used to link a metaproteomics analysis workflow to a specific peptide sequence and related information

    Attributes
    ----------
    all_proteins : Set['GeneProduct']
        the list of protein identifiers that are associated with the peptide sequence
    best_protein : Optional['GeneProduct']
        the specific protein identifier most correctly associated with the peptide sequence
    min_q_value : Optional['xsd:float']
        smallest Q-Value associated with the peptide sequence as provided by MSGFPlus tool
    peptide_sequence : Optional[str]
        null
    peptide_spectral_count : Optional[int]
        sum of filter passing MS2 spectra associated with the peptide sequence within a given LC-MS/MS data file
    peptide_sum_masic_abundance : Optional[int]
        combined MS1 extracted ion chromatograms derived from MS2 spectra associated with the peptide sequence from a given LC-MS/MS data file using the MASIC tool
    """

    all_proteins: Set["GeneProduct"]
    best_protein: Optional["GeneProduct"]
    min_q_value: Optional["xsd:float"]
    peptide_sequence: Optional[str]
    peptide_spectral_count: Optional[int]
    peptide_sum_masic_abundance: Optional[int]


class ProteinQuantification(DocumentTemplate):
    """This is used to link a metaproteomics analysis workflow to a specific protein

    Attributes
    ----------
    all_proteins : Set['GeneProduct']
        the list of protein identifiers that are associated with the peptide sequence
    best_protein : Optional['GeneProduct']
        the specific protein identifier most correctly associated with the peptide sequence
    peptide_sequence_count : Optional[int]
        count of peptide sequences grouped to the best_protein
    protein_spectral_count : Optional[int]
        sum of filter passing MS2 spectra associated with the best protein within a given LC-MS/MS data file
    protein_sum_masic_abundance : Optional[int]
        combined MS1 extracted ion chromatograms derived from MS2 spectra associated with the best protein from a given LC-MS/MS data file using the MASIC tool
    """

    all_proteins: Set["GeneProduct"]
    best_protein: Optional["GeneProduct"]
    peptide_sequence_count: Optional[int]
    protein_spectral_count: Optional[int]
    protein_sum_masic_abundance: Optional[int]


class MetaboliteQuantification(DocumentTemplate):
    """This is used to link a metabolomics analysis workflow to a specific metabolite

    Attributes
    ----------
    alternative_identifiers : Set[str]
        A list of alternative identifiers for the entity.
    highest_similarity_score : Optional['xsd:float']
        TODO: Yuri to fill in
    metabolite_quantified : Optional['ChemicalEntity']
        the specific metabolite identifier
    """

    alternative_identifiers: Set[str]
    highest_similarity_score: Optional["xsd:float"]
    metabolite_quantified: Optional["ChemicalEntity"]


class UrlValue(AttributeValue):
    """A value that is a string that conforms to URL syntax"""


class ImageValue(AttributeValue):
    """An attribute value representing an image.

    Attributes
    ----------
    description : Optional[str]
        a human-readable description of a thing
    display_order : Optional[str]
        When rendering information, this attribute to specify the order in which the information should be rendered.
    url : Optional[str]
        null
    """

    description: Optional[str]
    display_order: Optional[str]
    url: Optional[str]


class TextValue(AttributeValue):
    """A basic string value

    Attributes
    ----------
    language : Optional['xsd:language']
        Should use ISO 639-1 code e.g. "en", "fr"
    """

    language: Optional["xsd:language"]


class DataObject(NamedThing):
    """An object that primarily consists of symbols that represent information.   Files, records, and omics data are examples of data objects.

    Attributes
    ----------
    compression_type : Optional[str]
        If provided, specifies the compression type
    data_object_type : Optional[str]
        The type of file represented by the data object.
    description : Optional[str]
        a human-readable description of a thing
    file_size_bytes : Optional['xsd:long']
        Size of the file in bytes
    md5_checksum : Optional[str]
        MD5 checksum of file (pre-compressed)
    name : Optional[str]
        A human readable label for an entity
    type : Optional[str]
        An optional string that specifies the type object.  This is used to allow for searches for different kinds of objects.
    url : Optional[str]
        null
    was_generated_by : Optional['Activity']
        null
    """

    _key = LexicalKey(["id"])
    compression_type: Optional[str]
    data_object_type: Optional[str]
    description: Optional[str]
    file_size_bytes: Optional["xsd:long"]
    md5_checksum: Optional[str]
    name: Optional[str]
    type: Optional[str]
    url: Optional[str]
    was_generated_by: Optional["Activity"]


class TimestampValue(AttributeValue):
    """A value that is a timestamp. The range should be ISO-8601"""


class OmicsProcessing(BiosampleProcessing):
    """The methods and processes used to generate omics data from a biosample or organism.

    Attributes
    ----------
    GOLD_sequencing_project_identifiers : Set['xsd:anyURI']
        identifiers for corresponding sequencing project in GOLD
    INSDC_experiment_identifiers : Set['xsd:anyURI']
        null
    add_date : Optional[str]
        The date on which the information was added to the database.
    chimera_check : Optional['TextValue']
        A chimeric sequence, or chimera for short, is a sequence comprised of two or more phylogenetically distinct parent sequences. Chimeras are usually PCR artifacts thought to occur when a prematurely terminated amplicon reanneals to a foreign DNA strand and is copied to completion in the following PCR cycles. The point at which the chimeric sequence changes from one parent to the next is called the breakpoint or conversion point
    has_input : Set['NamedThing']
        An input to a process.
    has_output : Set['NamedThing']
        An output biosample to a processing step
    instrument_name : Optional[str]
        The name of the instrument that was used for processing the sample.

    mod_date : Optional[str]
        The last date on which the database information was modified.
    ncbi_project_name : Optional[str]
        null
    nucl_acid_amp : Optional['TextValue']
        A link to a literature reference, electronic resource or a standard operating procedure (SOP), that describes the enzymatic amplification (PCR, TMA, NASBA) of specific nucleic acids
    nucl_acid_ext : Optional['TextValue']
        A link to a literature reference, electronic resource or a standard operating procedure (SOP), that describes the material separation to recover the nucleic acid fraction from a sample
    omics_type : Optional['ControlledTermValue']
        The type of omics data
    part_of : Set['NamedThing']
        Links a resource to another resource that either logically or physically includes it.
    pcr_cond : Optional['TextValue']
        Description of reaction conditions and components of PCR in the form of  'initial denaturation:94degC_1.5min; annealing=...'
    pcr_primers : Optional['TextValue']
        PCR primers that were used to amplify the sequence of the targeted gene, locus or subfragment. This field should contain all the primers used for a single PCR reaction if multiple forward or reverse primers are present in a single PCR reaction. The primer sequence should be reported in uppercase letters
    principal_investigator : Optional['PersonValue']
        Principal Investigator who led the study and/or generated the dataset.
    processing_institution : Optional[str]
        The organization that processed the sample.
    samp_vol_we_dna_ext : Optional['QuantityValue']
        Volume (ml), weight (g) of processed sample, or surface area swabbed from sample for DNA extraction
    seq_meth : Optional['TextValue']
        Sequencing method used; e.g. Sanger, pyrosequencing, ABI-solid
    seq_quality_check : Optional['TextValue']
        Indicate if the sequence has been called by automatic systems (none) or undergone a manual editing procedure (e.g. by inspecting the raw data or chromatograms). Applied only for sequences that are not submitted to SRA,ENA or DRA
    target_gene : Optional['TextValue']
        Targeted gene or locus name for marker gene studies
    target_subfragment : Optional['TextValue']
        Name of subfragment of a gene or locus. Important to e.g. identify special regions on marker genes like V6 on 16S rRNA
    type : Optional[str]
        An optional string that specifies the type object.  This is used to allow for searches for different kinds of objects.
    """

    _key = LexicalKey(["id"])
    GOLD_sequencing_project_identifiers: Set["xsd:anyURI"]
    INSDC_experiment_identifiers: Set["xsd:anyURI"]
    add_date: Optional[str]
    chimera_check: Optional["TextValue"]
    has_input: Set["NamedThing"]
    has_output: Set["NamedThing"]
    instrument_name: Optional[str]
    mod_date: Optional[str]
    ncbi_project_name: Optional[str]
    nucl_acid_amp: Optional["TextValue"]
    nucl_acid_ext: Optional["TextValue"]
    omics_type: Optional["ControlledTermValue"]
    part_of: Set["NamedThing"]
    pcr_cond: Optional["TextValue"]
    pcr_primers: Optional["TextValue"]
    principal_investigator: Optional["PersonValue"]
    processing_institution: Optional[str]
    samp_vol_we_dna_ext: Optional["QuantityValue"]
    seq_meth: Optional["TextValue"]
    seq_quality_check: Optional["TextValue"]
    target_gene: Optional["TextValue"]
    target_subfragment: Optional["TextValue"]
    type: Optional[str]


class FunctionalAnnotationTerm(OntologyClass):
    """Abstract grouping class for any term/descriptor that can be applied to a functional unit of a genome (protein, ncRNA, complex)."""

    _key = LexicalKey(["id"])
    _abstract = []


class Pathway(FunctionalAnnotationTerm):
    """A pathway is a sequence of steps/reactions carried out by an organism or community of organisms

    Attributes
    ----------
    has_part : Set['Reaction']
        A pathway can be broken down to a series of reaction step
    """

    _key = LexicalKey(["id"])
    has_part: Set["Reaction"]


class OrthologyGroup(FunctionalAnnotationTerm):
    """A set of genes or gene products in which all members are orthologous"""

    _key = LexicalKey(["id"])


class Reaction(FunctionalAnnotationTerm):
    """An individual biochemical transformation carried out by a functional unit of an organism, in which a collection of substrates are transformed into a collection of products. Can also represent transporters

    Attributes
    ----------
    direction : Optional[str]
        One of l->r, r->l, bidirectional, neutral
    is_balanced : Optional[bool]
        null
    is_diastereoselective : Optional[bool]
        null
    is_fully_characterized : Optional[bool]
        False if includes R-groups
    is_stereo : Optional[bool]
        null
    is_transport : Optional[bool]
        null
    left_participants : Set['ReactionParticipant']
        null
    right_participants : Set['ReactionParticipant']
        null
    smarts_string : Optional[str]
        null
    """

    _key = LexicalKey(["id"])
    direction: Optional[str]
    is_balanced: Optional[bool]
    is_diastereoselective: Optional[bool]
    is_fully_characterized: Optional[bool]
    is_stereo: Optional[bool]
    is_transport: Optional[bool]
    left_participants: Set["ReactionParticipant"]
    right_participants: Set["ReactionParticipant"]
    smarts_string: Optional[str]


class WorkflowExecutionActivity(Activity):
    """Represents an instance of an execution of a particular workflow

    Attributes
    ----------
    ended_at_time : Optional[datetime]
        null
    execution_resource : Optional[str]
        Example: NERSC-Cori
    git_url : Optional[str]
        Example: https://github.com/microbiomedata/mg_annotation/releases/tag/0.1
    has_input : Set['NamedThing']
        An input to a process.
    has_output : Set['NamedThing']
        An output biosample to a processing step
    part_of : Set['NamedThing']
        Links a resource to another resource that either logically or physically includes it.
    started_at_time : Optional[datetime]
        null
    type : Optional[str]
        An optional string that specifies the type object.  This is used to allow for searches for different kinds of objects.
    was_associated_with : Optional['Agent']
        null
    was_informed_by : Optional['Activity']
        null
    """

    _key = LexicalKey(["id"])
    ended_at_time: Optional[datetime]
    execution_resource: Optional[str]
    git_url: Optional[str]
    has_input: Set["NamedThing"]
    has_output: Set["NamedThing"]
    part_of: Set["NamedThing"]
    started_at_time: Optional[datetime]
    type: Optional[str]
    was_associated_with: Optional["Agent"]
    was_informed_by: Optional["Activity"]


class MetatranscriptomeAnnotationActivity(WorkflowExecutionActivity):
    """"""

    _key = LexicalKey(["id"])


class ReadBasedAnalysisActivity(WorkflowExecutionActivity):
    """"""

    _key = LexicalKey(["id"])


class MAGsAnalysisActivity(WorkflowExecutionActivity):
    """

    Attributes
    ----------
    binned_contig_num : Optional[int]
        null
    input_contig_num : Optional[int]
        null
    lowDepth_contig_num : Optional[int]
        null
    mags_list : Set['MAGBin']
        null
    too_short_contig_num : Optional[int]
        null
    unbinned_contig_num : Optional[int]
        null
    """

    _key = LexicalKey(["id"])
    binned_contig_num: Optional[int]
    input_contig_num: Optional[int]
    lowDepth_contig_num: Optional[int]
    mags_list: Set["MAGBin"]
    too_short_contig_num: Optional[int]
    unbinned_contig_num: Optional[int]


class ReadQCAnalysisActivity(WorkflowExecutionActivity):
    """

    Attributes
    ----------
    has_input : Set['NamedThing']
        An input to a process.
    has_output : Set['NamedThing']
        An output biosample to a processing step
    input_base_count : Optional['xsd:float']
        The nucleotide base count number of input reads for QC analysis.
    input_read_bases : Optional['xsd:float']
        TODO
    input_read_count : Optional['xsd:float']
        The sequence count number of input reads for QC analysis.
    output_base_count : Optional['xsd:float']
        After QC analysis nucleotide base count number.
    output_read_bases : Optional['xsd:float']
        TODO
    output_read_count : Optional['xsd:float']
        After QC analysis sequence count number.
    """

    _key = LexicalKey(["id"])
    has_input: Set["NamedThing"]
    has_output: Set["NamedThing"]
    input_base_count: Optional["xsd:float"]
    input_read_bases: Optional["xsd:float"]
    input_read_count: Optional["xsd:float"]
    output_base_count: Optional["xsd:float"]
    output_read_bases: Optional["xsd:float"]
    output_read_count: Optional["xsd:float"]


class MetatranscriptomeAssembly(WorkflowExecutionActivity):
    """

    Attributes
    ----------
    INSDC_assembly_identifiers : Optional[str]
        null
    asm_score : Optional['xsd:float']
        A score for comparing metagenomic assembly quality from same sample.
    contig_bp : Optional['xsd:float']
        Total size in bp of all contigs.
    contigs : Optional['xsd:float']
        The sum of the (length*log(length)) of all contigs, times some constant.  Increase the contiguity, the score will increase
    ctg_L50 : Optional['xsd:float']
        Given a set of contigs, the L50 is defined as the sequence length of the shortest contig at 50% of the total genome length.
    ctg_L90 : Optional['xsd:float']
        The L90 statistic is less than or equal to the L50 statistic; it is the length for which the collection of all contigs of that length or longer contains at least 90% of the sum of the lengths of all contigs.
    ctg_N50 : Optional['xsd:float']
        Given a set of contigs, each with its own length, the N50 count is defined as the smallest number of contigs whose length sum makes up half of genome size.
    ctg_N90 : Optional['xsd:float']
        Given a set of contigs, each with its own length, the N90 count is defined as the smallest number of contigs whose length sum makes up 90% of genome size.
    ctg_logsum : Optional['xsd:float']
        Maximum contig length.
    ctg_max : Optional['xsd:float']
        Maximum contig length.
    ctg_powsum : Optional['xsd:float']
        Powersum of all contigs is the same as logsum except that it uses the sum of (length*(length^P)) for some power P (default P=0.25).
    gap_pct : Optional['xsd:float']
        The gap size percentage of all scaffolds.
    gc_avg : Optional['xsd:float']
        Average of GC content of all contigs.
    gc_std : Optional['xsd:float']
        Standard deviation of GC content of all contigs.
    num_aligned_reads : Optional['xsd:float']
        The sequence count number of input reads aligned to assembled contigs.
    num_input_reads : Optional['xsd:float']
        The sequence count number of input reads for assembly.
    scaf_L50 : Optional['xsd:float']
        Given a set of scaffolds, the L50 is defined as the sequence length of the shortest scaffold at 50% of the total genome length.
    scaf_L90 : Optional['xsd:float']
        The L90 statistic is less than or equal to the L50 statistic; it is the length for which the collection of all scaffolds of that length or longer contains at least 90% of the sum of the lengths of all scaffolds.
    scaf_N50 : Optional['xsd:float']
        Given a set of scaffolds, each with its own length, the N50 count is defined as the smallest number of scaffolds whose length sum makes up half of genome size.
    scaf_N90 : Optional['xsd:float']
        Given a set of scaffolds, each with its own length, the N90 count is defined as the smallest number of scaffolds whose length sum makes up 90% of genome size.
    scaf_bp : Optional['xsd:float']
        Total size in bp of all scaffolds.
    scaf_l_gt50K : Optional['xsd:float']
        Total size in bp of all scaffolds greater than 50 KB.
    scaf_logsum : Optional['xsd:float']
        The sum of the (length*log(length)) of all scaffolds, times some constant.  Increase the contiguity, the score will increase
    scaf_max : Optional['xsd:float']
        Maximum scaffold length.
    scaf_n_gt50K : Optional['xsd:float']
        Total sequence count of scaffolds greater than 50 KB.
    scaf_pct_gt50K : Optional['xsd:float']
        Total sequence size percentage of scaffolds greater than 50 KB.
    scaf_powsum : Optional['xsd:float']
        Powersum of all scaffolds is the same as logsum except that it uses the sum of (length*(length^P)) for some power P (default P=0.25).
    scaffolds : Optional['xsd:float']
        Total sequence count of all scaffolds.
    """

    _key = LexicalKey(["id"])
    INSDC_assembly_identifiers: Optional[str]
    asm_score: Optional["xsd:float"]
    contig_bp: Optional["xsd:float"]
    contigs: Optional["xsd:float"]
    ctg_L50: Optional["xsd:float"]
    ctg_L90: Optional["xsd:float"]
    ctg_N50: Optional["xsd:float"]
    ctg_N90: Optional["xsd:float"]
    ctg_logsum: Optional["xsd:float"]
    ctg_max: Optional["xsd:float"]
    ctg_powsum: Optional["xsd:float"]
    gap_pct: Optional["xsd:float"]
    gc_avg: Optional["xsd:float"]
    gc_std: Optional["xsd:float"]
    num_aligned_reads: Optional["xsd:float"]
    num_input_reads: Optional["xsd:float"]
    scaf_L50: Optional["xsd:float"]
    scaf_L90: Optional["xsd:float"]
    scaf_N50: Optional["xsd:float"]
    scaf_N90: Optional["xsd:float"]
    scaf_bp: Optional["xsd:float"]
    scaf_l_gt50K: Optional["xsd:float"]
    scaf_logsum: Optional["xsd:float"]
    scaf_max: Optional["xsd:float"]
    scaf_n_gt50K: Optional["xsd:float"]
    scaf_pct_gt50K: Optional["xsd:float"]
    scaf_powsum: Optional["xsd:float"]
    scaffolds: Optional["xsd:float"]


class MetagenomeAssembly(WorkflowExecutionActivity):
    """

    Attributes
    ----------
    INSDC_assembly_identifiers : Optional[str]
        null
    asm_score : Optional['xsd:float']
        A score for comparing metagenomic assembly quality from same sample.
    contig_bp : Optional['xsd:float']
        Total size in bp of all contigs.
    contigs : Optional['xsd:float']
        The sum of the (length*log(length)) of all contigs, times some constant.  Increase the contiguity, the score will increase
    ctg_L50 : Optional['xsd:float']
        Given a set of contigs, the L50 is defined as the sequence length of the shortest contig at 50% of the total genome length.
    ctg_L90 : Optional['xsd:float']
        The L90 statistic is less than or equal to the L50 statistic; it is the length for which the collection of all contigs of that length or longer contains at least 90% of the sum of the lengths of all contigs.
    ctg_N50 : Optional['xsd:float']
        Given a set of contigs, each with its own length, the N50 count is defined as the smallest number of contigs whose length sum makes up half of genome size.
    ctg_N90 : Optional['xsd:float']
        Given a set of contigs, each with its own length, the N90 count is defined as the smallest number of contigs whose length sum makes up 90% of genome size.
    ctg_logsum : Optional['xsd:float']
        Maximum contig length.
    ctg_max : Optional['xsd:float']
        Maximum contig length.
    ctg_powsum : Optional['xsd:float']
        Powersum of all contigs is the same as logsum except that it uses the sum of (length*(length^P)) for some power P (default P=0.25).
    gap_pct : Optional['xsd:float']
        The gap size percentage of all scaffolds.
    gc_avg : Optional['xsd:float']
        Average of GC content of all contigs.
    gc_std : Optional['xsd:float']
        Standard deviation of GC content of all contigs.
    num_aligned_reads : Optional['xsd:float']
        The sequence count number of input reads aligned to assembled contigs.
    num_input_reads : Optional['xsd:float']
        The sequence count number of input reads for assembly.
    scaf_L50 : Optional['xsd:float']
        Given a set of scaffolds, the L50 is defined as the sequence length of the shortest scaffold at 50% of the total genome length.
    scaf_L90 : Optional['xsd:float']
        The L90 statistic is less than or equal to the L50 statistic; it is the length for which the collection of all scaffolds of that length or longer contains at least 90% of the sum of the lengths of all scaffolds.
    scaf_N50 : Optional['xsd:float']
        Given a set of scaffolds, each with its own length, the N50 count is defined as the smallest number of scaffolds whose length sum makes up half of genome size.
    scaf_N90 : Optional['xsd:float']
        Given a set of scaffolds, each with its own length, the N90 count is defined as the smallest number of scaffolds whose length sum makes up 90% of genome size.
    scaf_bp : Optional['xsd:float']
        Total size in bp of all scaffolds.
    scaf_l_gt50K : Optional['xsd:float']
        Total size in bp of all scaffolds greater than 50 KB.
    scaf_logsum : Optional['xsd:float']
        The sum of the (length*log(length)) of all scaffolds, times some constant.  Increase the contiguity, the score will increase
    scaf_max : Optional['xsd:float']
        Maximum scaffold length.
    scaf_n_gt50K : Optional['xsd:float']
        Total sequence count of scaffolds greater than 50 KB.
    scaf_pct_gt50K : Optional['xsd:float']
        Total sequence size percentage of scaffolds greater than 50 KB.
    scaf_powsum : Optional['xsd:float']
        Powersum of all scaffolds is the same as logsum except that it uses the sum of (length*(length^P)) for some power P (default P=0.25).
    scaffolds : Optional['xsd:float']
        Total sequence count of all scaffolds.
    """

    _key = LexicalKey(["id"])
    INSDC_assembly_identifiers: Optional[str]
    asm_score: Optional["xsd:float"]
    contig_bp: Optional["xsd:float"]
    contigs: Optional["xsd:float"]
    ctg_L50: Optional["xsd:float"]
    ctg_L90: Optional["xsd:float"]
    ctg_N50: Optional["xsd:float"]
    ctg_N90: Optional["xsd:float"]
    ctg_logsum: Optional["xsd:float"]
    ctg_max: Optional["xsd:float"]
    ctg_powsum: Optional["xsd:float"]
    gap_pct: Optional["xsd:float"]
    gc_avg: Optional["xsd:float"]
    gc_std: Optional["xsd:float"]
    num_aligned_reads: Optional["xsd:float"]
    num_input_reads: Optional["xsd:float"]
    scaf_L50: Optional["xsd:float"]
    scaf_L90: Optional["xsd:float"]
    scaf_N50: Optional["xsd:float"]
    scaf_N90: Optional["xsd:float"]
    scaf_bp: Optional["xsd:float"]
    scaf_l_gt50K: Optional["xsd:float"]
    scaf_logsum: Optional["xsd:float"]
    scaf_max: Optional["xsd:float"]
    scaf_n_gt50K: Optional["xsd:float"]
    scaf_pct_gt50K: Optional["xsd:float"]
    scaf_powsum: Optional["xsd:float"]
    scaffolds: Optional["xsd:float"]


class NomAnalysisActivity(WorkflowExecutionActivity):
    """

    Attributes
    ----------
    has_calibration : Optional[str]
        TODO: Yuri to fill in
    used : Optional[str]
        null
    """

    _key = LexicalKey(["id"])
    has_calibration: Optional[str]
    used: Optional[str]


class MetabolomicsAnalysisActivity(WorkflowExecutionActivity):
    """

    Attributes
    ----------
    has_calibration : Optional[str]
        TODO: Yuri to fill in
    has_metabolite_quantifications : Set['MetaboliteQuantification']
        null
    used : Optional[str]
        null
    """

    _key = LexicalKey(["id"])
    has_calibration: Optional[str]
    has_metabolite_quantifications: Set["MetaboliteQuantification"]
    used: Optional[str]


class MetatranscriptomeActivity(WorkflowExecutionActivity):
    """A metatranscriptome activity that e.g. pools assembly and annotation activity."""

    _key = LexicalKey(["id"])


class MetaproteomicsAnalysisActivity(WorkflowExecutionActivity):
    """

    Attributes
    ----------
    has_peptide_quantifications : Set['PeptideQuantification']
        null
    used : Optional[str]
        null
    """

    _key = LexicalKey(["id"])
    has_peptide_quantifications: Set["PeptideQuantification"]
    used: Optional[str]


class MetagenomeAnnotationActivity(WorkflowExecutionActivity):
    """"""

    _key = LexicalKey(["id"])
