import arcpy
import datetime
import sys, os, csv

def Table2Dict(inputFeature,key_field,field_names):
    """Convert table to nested dictionary.

    The key_field -- feature ID for input feature class
    field_names -- selected field names for input feature class
    return outDict={key_field:{field_name1:value1,field_name2:value2...}}
    """
    outDict={}
    field_names.insert(0,key_field)
    with arcpy.da.SearchCursor(inputFeature,field_names) as cursor:
        for row in cursor:
            outDict[row[0]]=dict(zip(field_names[1:],row[1:]))
    return outDict

def JoinTable(inFeature, inField, joinTable, joinField):
    """Join table with other table files.

    inFeature -- input feature
    inField -- key field in input feature
    joinTable -- join table to be appended to input feature
    joinField -- key field in join table
    outFeature -- output feature
    """
    import arcpy, traceback, sys
    arcpy.env.workspace = sys.path[0]
    arcpy.env.overwriteOutput = True

    if not joinTable.endswith('.dbf'):
        joinDBF = joinTable.split('.',1)[0]+".dbf"
        outputFolder = sys.path[0]
        arcpy.TableToTable_conversion(joinTable, outputFolder, joinDBF)
    arcpy.JoinField_management (inFeature, inField, joinDBF, joinField)
    arcpy.Delete_management (joinDBF)


def SpatialJoin2Dict(targetFC,keyID, matchFC,thresholdDist):
    """Spatially join two feature classes based on distance and return a join dictionary.

    targetFC -- target feature class to be spatially joined
    keyID -- key ID of target feature class
    matchFC -- matching feature class
    thresholdDist -- threshold searching distance for spatial join

    return a join dictionary in a format as join_Dict = {keyID:{'JOIN_FID':####}}
    """
    joinFC=os.path.join(sys.path[0],"spatialJoin.shp")
    arcpy.SpatialJoin_analysis(targetFC, matchFC, joinFC,join_operation="JOIN_ONE_TO_MANY",match_option="CLOSEST",search_radius=thresholdDist)
    join_fields=['JOIN_FID']
    join_Dict=AU.Table2Dict(joinFC,keyID,join_fields)
    arcpy.Delete_management(joinFC)
    return join_Dict

def PointToPointMatch(targetFC, tgtKID, tmKID, matchFC, matchKID, join_Dict):
    """Spatially match target feature class to another feature class.

    targetFC -- target feature class
    tgtKID -- key ID of target feature class
    tmKID -- field to be matched with match feature class
    matchFC -- feature class to be matched with targetFC
    matchKID -- corresponding match field of matchFC
    join_Dict -- a dictionary of spatial join between target feature class and matching feature class
                 format: join_Dict = {keyID:{'JOIN_FID':####}}

    return a match dictionary in a format as match_Dict = {"TARGETID":[01,02...],"MATCHID":[001,002..]}
    """
    match_Dict={"TARGETID":[],"mOBJECTID":[], "MATCHID":[]}
    with arcpy.da.SearchCursor(matchFC,['OBJECTID', matchKID], sql_clause=(None, 'ORDER BY OBJECTID')) as cursor:
        MatchFCDic = {}
        for row in cursor:
            MatchFCDic[row[0]]=row[1]

    with arcpy.da.UpdateCursor(targetFC,[tgtKID,tmKID], sql_clause=(None, 'ORDER BY '+ tgtKID)) as cursor:
        for row in cursor:
            match_Dict["TARGETID"].append(row[0])
            if join_Dict[row[0]]['JOIN_FID'] != -1:
                row[1]=MatchFCDic[join_Dict[row[0]]['JOIN_FID']]
                match_Dict["mOBJECTID"].append(join_Dict[row[0]]['JOIN_FID'])
                match_Dict["MATCHID"].append(row[1])
            else:
                match_Dict["mOBJECTID"].append(None)
                match_Dict["MATCHID"].append(None)
            cursor.updateRow(row)
    return match_Dict

def PointToPointSnap(targetFC,keyID,matchFC,join_Dict):
    """Move targets to locations of corresponding matching features.

    targetFC -- target feature class to be moved
    keyID -- key ID of target feature class
    matchFC -- matching feature class
    join_Dict -- a dictionary of spatial join between target feature class and matching feature class
                 format: join_Dict = {keyID:{'JOIN_FID':####}}

    return a location dictionary indicates the original locations of changed records,
    in a format as loc_Dict = {"FromX":[...], "FromY":[...]}
    """
    loc_Dict = {"FromX":[], "FromY":[]}
    match_fields=['SHAPE@X','SHAPE@Y']
    matchFC_Dict=AU.Table2Dict(matchFC,'OBJECTID',match_fields)

    with arcpy.da.UpdateCursor(targetFC,[keyID,'SHAPE@X','SHAPE@Y'],sql_clause=(None, 'ORDER BY '+keyID))as cursor:
        for row in cursor:
            if join_Dict[row[0]]['JOIN_FID'] != -1:
                loc_Dict["FromX"].append(row[1])
                loc_Dict["FromY"].append(row[2])
                row[1]=matchFC_Dict[join_Dict[row[0]]['JOIN_FID']]['SHAPE@X']
                row[2]=matchFC_Dict[join_Dict[row[0]]['JOIN_FID']]['SHAPE@Y']
                cursor.updateRow(row)
            else:
                loc_Dict["FromX"].append(None)
                loc_Dict["FromY"].append(None)
    return loc_Dict

def CheckAttachments(my_features, feature_ID, my_attachments, attach_ID):
    """Count the number of attachments.

    my_features -- feature class of which attachments are counted
    feature_ID -- key ID of my_features to be based on join
    my_attachments -- attachment table of feature class
    attach_ID -- key ID of my_attachments to match my_features

    return an attachment dictionary in a format as attach_Dict = {"AttachCnt":[1,2,..]}
    in the feature ID order
    """
    attach_Dict = {"AttachCnt":[]}
    attach_list = []

    with arcpy.da.SearchCursor(my_attachments,[attach_ID])as rows:
        for row in rows:
            attach_list.append(row[0])

    with arcpy.da.SearchCursor(my_features,[feature_ID]) as frows:
        for frow in sorted(frows):
            attach_Dict["AttachCnt"].append(attach_list.count(frow[0]))
    return attach_Dict

def MultiRecCheck(inputFC, keyID, relfield, checkfields):
    """Check the disagreement of checkfields between records which are linked by relfield.

    inputFC -- input feature class
    keyID -- identification id of input feature class
    relfield -- relationship field used to link records
    checkfields -- fields to be checked

    return a dictionary in a format as: MatchDic = {keyID1:0, keyID2:1...} in which 0 represents no error and 1 for error.
    """
    dataDic = {}
    with arcpy.da.SearchCursor(inputFC,[keyID]+checkfields,sql_clause=(None, 'ORDER BY '+keyID))as cursor:
        for row in cursor:
            checkDic = dict(zip(checkfields,row[1:]))
            dataDic[row[0]]=checkDic

    MatchDic = {} #'TimeMErr' for error, 'OK' for no error,'(relfield)Err'for Reference field error
    with arcpy.da.SearchCursor(inputFC, [keyID,relfield],sql_clause=(None, 'ORDER BY '+relfield))as cursor:
        initLZID = -999
        for row in cursor:
            if row[1] != initLZID:
                initLZID = row[1]
                if row[1] is None or '_' not in row[1]:
                    MatchDic[row[0]] = None
                else:
                    signlist = row[1].split('_')
                    for item in signlist:
                        if int(item) not in dataDic:
                            MatchDic[row[0]] = relfield+'Err'
                    if row[0] not in MatchDic:
                        if all(dataDic[int(item)] == dataDic[int(signlist[0])] for item in signlist):
                            MatchDic[row[0]]='OK'
                        else:
                            MatchDic[row[0]]='MErr'

                lastV = MatchDic[row[0]]
            else:
                MatchDic[row[0]]=lastV
    return MatchDic

def MultiTargetCount(matchFC, matchID, MatchIDList):
    """Count the number of matched targets for each matching feature.

    matchFC -- matching feature class of target feature class
    matchID -- keyID of matching feature class
    MatchIDList -- a list of IDs for the matching feature for the target feature
    """
    pole_check_Dict={"POLEID":[],"SignCount":[]}
    with arcpy.da.SearchCursor(matchFC, matchID) as cursor:
        for row in sorted(cursor):
            pole_check_Dict["POLEID"].append(row[0])
            pole_check_Dict["SignCount"].append(MatchIDList.count(row[0]))
    return pole_check_Dict


#Populate new SIGNID from number
def PopNewIDfromNum(input_FC, IDname):
    startValue = 1
    with arcpy.da.UpdateCursor(input_FC,[IDname],sql_clause=(None, 'ORDER BY OBJECTID'))as cursor:
        for row in cursor:
            row[0]=startValue
            startValue += 1
            cursor.updateRow(row)

#Populate SIGNID = OBJECTID
def PopNewIDfromField(input_FC, IDname, refField):
    with arcpy.da.UpdateCursor(input_FC,[refField,IDname],sql_clause=(None, 'ORDER BY '+refField))as cursor:
        for row in cursor:
            row[1]=row[0]
            cursor.updateRow(row)

# Assign ObjectID to null ID value
def AssignNewIDfromOBJECTID(input_FC,IDname):
    with arcpy.da.UpdateCursor(input_FC,['OBJECTID',IDname],sql_clause=(None, 'ORDER BY OBJECTID'))as cursor:
        for row in cursor:
            if row[1] is None:
                row[1]=row[0]
                cursor.updateRow(row)

# Assign a number value to the null ID value(starting from maximum+1)
def AssignNewIDfromNum(input_FC, IDname):
    with arcpy.da.SearchCursor(input_FC,[IDname],sql_clause=(None, 'ORDER BY '+IDname)) as cursor:
        valueRange =[]
        for row in cursor:
            if row[0]is not None:
                valueRange.append(row[0])
        maxV = max(valueRange)

    with arcpy.da.UpdateCursor(input_FC,[IDname],sql_clause=(None, 'ORDER BY '+IDname)) as cursor:
        for row in cursor:
            if row[0]is None:
                maxV+=1
                row[0]=maxV
                cursor.updateRow(row)
