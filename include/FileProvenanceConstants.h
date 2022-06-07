/*
 * This file is part of ePipe
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
 *
 * ePipe is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * ePipe is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef FILEPROVENANCECONSTANTS_H
#define FILEPROVENANCECONSTANTS_H

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include "tables/FileProvenanceLogTable.h"
#include "FileProvenanceConstantsRaw.h"
#include "tables/INodeTable.h"

namespace FileProvenanceConstants {

  const std::string README_FILE = "README.md";
  
  const std::string TYPE_NONE = "NONE";
  const std::string TYPE_DATASET = "DATASET";
  const std::string TYPE_HIVE = "HIVE";
  const std::string TYPE_HIVE_PART = "HIVE_PART";

  const std::string ML_TYPE_FEATURE = "FEATURE";
  const std::string ML_TYPE_FEATURE_PART = "FEATURE_PART";
  const std::string ML_TYPE_TDATASET = "TRAINING_DATASET";
  const std::string ML_TYPE_TDATASET_PART = "TRAINING_DATASET_PART";
  const std::string ML_TYPE_EXPERIMENT = "EXPERIMENT";
  const std::string ML_TYPE_EXPERIMENT_PART = "EXPERIMENT_PART";
  const std::string ML_TYPE_MODEL = "MODEL";
  const std::string ML_TYPE_MODEL_PART = "MODEL_PART";

  const std::string XATTR = "xattr_prov";

  const std::string XATTR_PROJECT_IID = "project_iid"; //part of project core

  const std::string PROV_TYPE_STORE_NONE = "NONE";
  const std::string PROV_TYPE_STORE_STATE = "STATE";
  const std::string PROV_TYPE_STORE_ALL = "ALL";
  enum MLType {
    NONE,
    DATASET,

    HIVE,
    FEATURE,
    TRAINING_DATASET,
    EXPERIMENT,
    MODEL,

    HIVE_PART,
    FEATURE_PART,
    TRAINING_DATASET_PART,
    EXPERIMENT_PART,
    MODEL_PART
  };

  enum ProvOpStoreType {
    STORE_NONE,
    STORE_STATE,
    STORE_ALL
  };

  inline static const std::string MLTypeToStr(MLType mlType) {
    switch (mlType) {
      case NONE:
        return TYPE_NONE;
      case DATASET:
        return TYPE_DATASET;
      case HIVE:
        return TYPE_HIVE;
      case FEATURE:
        return ML_TYPE_FEATURE;
      case TRAINING_DATASET:
        return ML_TYPE_TDATASET;
      case EXPERIMENT:
        return ML_TYPE_EXPERIMENT;
      case MODEL:
        return ML_TYPE_MODEL;
      case HIVE_PART:
        return TYPE_HIVE_PART;
      case FEATURE_PART:
        return ML_TYPE_FEATURE_PART;
      case TRAINING_DATASET_PART:
        return ML_TYPE_TDATASET_PART;
      case EXPERIMENT_PART:
        return ML_TYPE_EXPERIMENT_PART;
      case MODEL_PART:
        return ML_TYPE_MODEL_PART;
      default:
        return TYPE_NONE;
    }
  };

  const std::string ELASTIC_NOP = "\n";
  const std::string ELASTIC_NOP2 = "\n\n";


  const std::string APP_SUBMITTED_STATE = "SUBMITTED";
  const std::string APP_RUNNING_STATE = "RUNNING";

  inline bool oneLvlDeep(FileProvenanceRow row) {
    return row.mDatasetId != row.mInodeId && row.mDatasetId == row.mParentId;
  }

  inline bool twoLvlDeep(FileProvenanceRow row) {
    return row.mDatasetId != row.mInodeId && row.mDatasetId != row.mParentId && row.mP1Name != "" && row.mP2Name == "";
  }

  inline bool onePlusLvlDeep(FileProvenanceRow row) {
    return row.mDatasetId != row.mInodeId && row.mDatasetId != row.mParentId;
  }

  inline bool twoPlusLvlDeep(FileProvenanceRow row) {
    return row.mDatasetId != row.mInodeId && row.mDatasetId != row.mParentId && row.mP1Name != "" && row.mP2Name != "";
  }

  inline std::string twoNameForAsset(FileProvenanceRow row) {
    std::stringstream  mlId;
    mlId << row.mParentName << "_" << row.mInodeName;
    return mlId.str();
  }

  inline std::string twoNameForPart(FileProvenanceRow row) {
    std::stringstream  mlId;
    if(row.mP2Name == "") {
      mlId << row.mP1Name << "_" << row.mParentName;
    } else {
      mlId << row.mP1Name << "_" << row.mP2Name;
    }
    return mlId.str();
  }

  inline std::string oneNameForPart(FileProvenanceRow row) {
    std::stringstream  mlId;
    if(row.mP1Name == "") {
      mlId << row.mParentName;
    } else {
      mlId << row.mP1Name;
    }
    return mlId.str();
  }

  inline bool isDataset(FileProvenanceRow row) {
    return row.mDatasetId == row.mInodeId;
  }

  inline bool isDatasetName1(FileProvenanceRow row, std::string part) {
    return row.mDatasetName == part;
  }

  inline bool isDatasetName2(FileProvenanceRow row, std::string part) {
    std::stringstream  mlDataset;
    mlDataset << row.mProjectName << "_" << part;
    return row.mDatasetName == mlDataset.str();
  }

  inline bool isReadmeFile(FileProvenanceRow row) {
    return row.mInodeName == README_FILE;
  }

  inline bool isMLModel(FileProvenanceRow row) {
    return isDatasetName1(row, "Models") && twoLvlDeep(row);
  }

  inline bool partOfMLModel(FileProvenanceRow row) {
    return isDatasetName1(row, "Models") && twoPlusLvlDeep(row);
  }

  inline std::string getMLModelId(FileProvenanceRow row) {
    return twoNameForAsset(row);
  }

  inline std::string getMLModelParentId(FileProvenanceRow row) {
    return twoNameForPart(row);
  }

  inline bool typeHive(FileProvenanceRow row) {
    return row.mProjectId == -1 && row.mDatasetName == row.mProjectName + ".db";
  }

  inline bool isHive(FileProvenanceRow row) {
    return typeHive(row) && row.mDatasetId == row.mParentId;
  }

  inline bool partOfHive(FileProvenanceRow row) {
    return typeHive(row) && row.mDatasetId != row.mParentId;
  }

  inline std::string featurestoreName(std::string projectName) {
    std::string auxProjectName = projectName;
    boost::to_lower(auxProjectName);
    return auxProjectName + "_featurestore.db";
  }

  inline bool isFeaturestore(std::string projectName, std::string datasetName) {
    return projectName != DONT_EXIST_STR() && datasetName != DONT_EXIST_STR()
    && datasetName == featurestoreName(projectName);
  }

  inline boost::optional<std::pair <std::string, int>> splitNameVersion(std::string val) {
    std::vector<std::string> strs;
    boost::split(strs, val, boost::is_any_of("_"));
    std::string name = "";
    int version;
    for(std::vector<std::string>::iterator it = strs.begin(); it != strs.end(); it++) {
      std::string part = *it;
      if(std::next(it) == strs.end()) {
        try {
          version = std::stoi(part);
          return std::make_pair(name, version);
        } catch(std::invalid_argument const &e) {
          LOG_WARN("problem with name_version:" << val << " name:" << name << " version:" << part);
          return boost::none;
        } catch(std::out_of_range const &e) {
          LOG_WARN("problem with name_version:" << val << " name:" << name << " version:" << part);
          return boost::none;
        }
      } else {
        if(name == "") {
          name = name + part;
        } else {
          name = name + std::string("_") + part;
        }
      }
    }
    //should not get here - removes warning
    return boost::none;
  }

  inline std::string trainingdatasetDirName(std::string projectName) {
    std::string auxProjectName = projectName;
    //boost::to_lower(auxProjectName);
    return auxProjectName + "_Training_Datasets";
  }

  inline bool isTrainingDataset(std::string projectName, std::string datasetName) {
    return projectName != DONT_EXIST_STR() && datasetName != DONT_EXIST_STR()
           && datasetName == trainingdatasetDirName(projectName);
  }

  inline bool isTrainingDataset(Int64 parentIId, Int64 datasetIId, std::string projectName, std::string datasetName) {
    return  isTrainingDataset(projectName, datasetName)
         && parentIId == datasetIId;
  }

  inline std::string featureViewArtifact(Int64 parentIId, Int64 datasetIId, Int64 featureViewIId, std::string inodeName) {
    if(parentIId == featureViewIId) {
      return "featureview";
    } else if(parentIId == datasetIId) {
      if(inodeName == ".featureview" || inodeName == "code" || inodeName == "transformation_functions") {
        LOG_DEBUG("skipping training dataset basic folder:" << inodeName);
        return DONT_EXIST_STR();
      } else {
        return "trainingdataset";
      }
    } else {
      return DONT_EXIST_STR();
    }
  }

  inline std::string featureStoreArtifact(Int64 parentIId, Int64 datasetIId, std::string inodeName) {
    if(parentIId == datasetIId) {
      if(inodeName == "code" || inodeName == "storage_connector_resources") {
        LOG_DEBUG("skipping feature store basic folder:" << inodeName);
        return DONT_EXIST_STR();
      } else {
        return "featuregroup";
      }
    } else {
      return DONT_EXIST_STR();
    }
  }

  inline Int64 getFeatureViewParentIId(Ndb* conn, INodeTable inodesTable, Int64 parentIId) {
    if (FeatureViewInodeCache::getInstance().contains(parentIId)) {
      return FeatureViewInodeCache::getInstance().get(parentIId);
    }

    //we do not know the partitionId
    LOG_DEBUG("feature view parent iid:" << parentIId);
    INodeRow row = inodesTable.get(conn, parentIId, ".featureviews", parentIId);
    FeatureViewInodeCache::getInstance().add(parentIId, row.mId);
    return row.mId;
  }

  inline std::string getFeatureStoreArtifact(Ndb* conn, INodeTable inodesTable,
                                             std::string projectName, std::string datasetName, std::string inodeName,
                                             Int64 datasetIId, Int64 parentIId) {
    if(FileProvenanceConstants::isTrainingDataset(projectName, datasetName)) {
      Int64 featureViewIId = getFeatureViewParentIId(conn, inodesTable, datasetIId);
      LOG_DEBUG("feature view iid:" << featureViewIId);
      return FileProvenanceConstants::featureViewArtifact(parentIId, datasetIId, featureViewIId, inodeName);
    } else if(FileProvenanceConstants::isFeaturestore(projectName, datasetName)) {
      return FileProvenanceConstants::featureStoreArtifact(parentIId, datasetIId, inodeName);
    } else {
      return DONT_EXIST_STR();
    }
  }

  inline bool typeMLFeature(FileProvenanceRow row) {
    return row.mProjectId == -1 && row.mDatasetName == featurestoreName(row.mProjectName);
  }

  inline bool isMLFeature(FileProvenanceRow row) {
    return typeMLFeature(row) && row.mDatasetId == row.mParentId;
  }

  inline bool partOfMLFeature(FileProvenanceRow row) {
    return typeMLFeature(row) && row.mDatasetId != row.mParentId;
  }

  inline std::string getMLFeatureId(FileProvenanceRow row) {
    return row.mInodeName;
  }

  inline std::string getMLFeatureParentId(FileProvenanceRow row) {
    return oneNameForPart(row);
  }

  inline bool isMLTDataset(FileProvenanceRow row) {
    return isDatasetName2(row, "Training_Datasets") && oneLvlDeep(row);
  }

  inline bool partOfMLTDataset(FileProvenanceRow row) {
    return isDatasetName2(row, "Training_Datasets") && onePlusLvlDeep(row);
  }

  inline std::string getMLTDatasetId(FileProvenanceRow row) {
    return row.mInodeName;
  }

  inline std::string getMLTDatasetParentId(FileProvenanceRow row) {
    return oneNameForPart(row);
  }

  inline bool isMLExperimentName(std::string name) {
    std::vector<std::string> strs;
    boost::split(strs,name,boost::is_any_of("_"));
    LOG_INFO("name:" << name << " size:" << strs.size());
    return boost::starts_with(name, "application_") && strs.size() == 4;
  }

  inline bool isMLExperiment(FileProvenanceRow row) {
    return isDatasetName1(row, "Experiments") && oneLvlDeep(row) 
      && isMLExperimentName(row.mInodeName);
  }

  inline bool partOfMLExperiment(FileProvenanceRow row) {
    return isDatasetName1(row, "Experiments") && onePlusLvlDeep(row) 
      && isMLExperimentName(row.mParentName);
  }

  inline std::string getMLExperimentId(FileProvenanceRow row) {
    return row.mInodeName;
  }

  inline std::string getMLExperimentParentId(FileProvenanceRow row) {
    return oneNameForPart(row);
  }

  inline std::pair <MLType, std::string> parseML(FileProvenanceRow row) {
    MLType mlType;
    std::string mlId;
    if(isReadmeFile(row)) {
      mlType = MLType::NONE;
      mlId = "";
    } else if(isMLFeature(row)) {
      mlType = MLType::FEATURE;
      mlId = getMLFeatureId(row);
    } else if(isMLTDataset(row)) {
      mlType = MLType::TRAINING_DATASET;
      mlId = getMLTDatasetId(row);
    } else if(isMLExperiment(row)) {
      mlType = MLType::EXPERIMENT;
      mlId = getMLExperimentId(row);
    } else if(isMLModel(row)) {
      mlType = MLType::MODEL;
      mlId = getMLModelId(row);
    } else if(isHive(row)) {
      mlType = MLType::HIVE;
      mlId = "";
    } else if(isDataset(row)) {
      mlType = MLType::DATASET;
      mlId = "";
    } else if(partOfMLFeature(row)) {
      mlType = MLType::FEATURE_PART;
      mlId = getMLFeatureParentId(row);
    } else if(partOfMLTDataset(row)) {
      mlType = MLType::TRAINING_DATASET_PART;
      mlId = getMLTDatasetParentId(row);
    } else if(partOfMLExperiment(row)) {
      mlType = MLType::EXPERIMENT_PART;
      mlId = getMLExperimentParentId(row);
    } else if(partOfMLModel(row)) {
      mlType = MLType::MODEL_PART;
      mlId = getMLModelParentId(row);
    } else if(partOfHive(row)) {
      mlType = MLType::HIVE_PART;
      mlId = "";
    } else {
      mlType = MLType::NONE;
      mlId = "";
    }
    return std::make_pair(mlType, mlId);
  }

  inline std::pair<ProvOpStoreType, Int64> provCore(std::string dsProvType) {
    rapidjson::Document provTypeDoc;
    if(provTypeDoc.Parse(dsProvType.c_str()).HasParseError()) {
      LOG_WARN("prov type could not be parsed:" << dsProvType);
      std::stringstream cause;
      cause << "prov type could not be parsed:" << dsProvType;
      throw std::logic_error(cause.str());
    } else {
      Int64 projectIID = provTypeDoc["project_iid"].GetInt64();
      std::string provType = provTypeDoc["prov_type"]["prov_status"].GetString();
      boost::to_upper(provType);
      if(provType == PROV_TYPE_STORE_STATE) {
        return std::make_pair(ProvOpStoreType::STORE_STATE, projectIID);
      } else if(provType == PROV_TYPE_STORE_ALL) {
        return std::make_pair(ProvOpStoreType::STORE_ALL, projectIID);
      } else if(provType == PROV_TYPE_STORE_NONE) {
        return std::make_pair(ProvOpStoreType::STORE_NONE, projectIID);
      } else {
        LOG_WARN("prov type not recognized:" << provType);
        std::stringstream cause;
        cause << "prov type not recognized:" << provType;
        throw std::logic_error(cause.str());
      }
    }
  }

  inline static const std::string provOpStoreTypeToStr(ProvOpStoreType type) {
    switch(type) {
      case STORE_NONE: return PROV_TYPE_STORE_NONE;
      case STORE_STATE: return PROV_TYPE_STORE_STATE;
      case STORE_ALL: return PROV_TYPE_STORE_ALL;
      default : {
        LOG_ERROR("ProvOpStoreType to string - enum not handled");
        std::stringstream cause;
        cause << "ProvOpStoreType to string - enum not handled";
        throw std::logic_error(cause.str());
      }
    }
  }

  inline static const std::string projectIndex(Int64 projectIId) {
    return std::to_string(projectIId) + "__file_prov";
  }
}

#endif /* FILEPROVENANCECONSTANTS_H */