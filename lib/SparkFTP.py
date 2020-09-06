#!/bin/python
# -*- coding: utf-8 -*-

##################
# SparkFTP class #
##################
class SparkFTP:
    import os
    from ftplib import FTP
    from ftplib import FTP_TLS
    from io import StringIO

    sparkContext = None
    ftpConnection = None
    isSecured = False

    def __init__(self, sparkContext):
        self.sparkContext = sparkContext

    class ZIP:
        import zipfile
        from io import StringIO

        zipBytes = None
        items = None

        def __init__(self, zipBytes):
            self.zipBytes = zipBytes
            zipObj = self._initZipObj(zipBytes)
            self.items = zipObj.namelist()

        def unzipItem(self, itemCode):
            if (type(itemCode) == int): itemIndex = itemCode
            else: itemIndex = self.items.index(itemCode)
            itemPath = self.items[itemIndex]
            zipObj = self._initZipObj(self.zipBytes)
            dataBytes = zipObj.read(itemPath)
            zipObj.close()
            return dataBytes

        def _initZipObj(self, zipBytes):
            zipBuff = self.StringIO()
            zipBuff.write(zipBytes)
            zipBuff.seek(0)
            zipObj = self.zipfile.ZipFile(zipBuff, mode='r')
            return zipObj

    """
    Workflow 01's methods - 1/2
    """

    def openFtpConnection(self, host="localhost", port=21, login="anonymous", password=None, timeout=600, isSecured=False):
        self.isSecured = isSecured
        if (isSecured == True):
            _ftpConnection = FTP_TLS(timeout=timeout)
            _ftpConnection.connect(host, port)
            _ftpConnection.auth()
            _ftpConnection.prot_p()
        else:
            _ftpConnection = self.FTP(timeout=timeout)
            _ftpConnection.connect(host, port)
        _ftpConnection.login(login, password)
        self.ftpConnection = _ftpConnection

    def closeFtpConnection(self):
        if (self.ftpConnection != None): self.ftpConnection.close()

    def scanFtpFolders(self, ftpParentFolder="/", recursive=False, startsWith=None, contains=None, endsWith=None):
        ftpSubFolders = self._scanFtpFolders(self.ftpConnection, ftpParentFolder, recursive, startsWith, contains, endsWith)
        return ftpSubFolders

    def scanFtpFiles(self, ftpParentFolder="/", recursive=False, startsWith=None, contains=None, endsWith=None):
        ftpFiles = self._scanFtpFiles(self.ftpConnection, ftpParentFolder, recursive, startsWith, contains, endsWith)
        return ftpFiles

    def ftpFileSize(self, ftpFilePath):
        ftpFileSize = self.ftpConnection.size(ftpFilePath)
        return ftpFileSize

    def moveFtpFile(self, sourceFtpFilePath, targetFtpFilePath):
        self.ftpConnection.rename(sourceFtpFilePath, targetFtpFilePath)

    def renameFtpFile(self, sourceFtpFilePath, targetFtpFilePath):
        self.moveFtpFile(sourceFtpFilePath, targetFtpFilePath)

    def deleteFtpFile(self, ftpFilePath):
        self.ftpConnection.delete(ftpFilePath)

    """
    Workflow 01's methods - 2/2
    """

    def ftpFileToBytes(self, ftpFilePath):
        dataStream = self.StringIO()
        self.ftpConnection.retrbinary("RETR " + ftpFilePath, dataStream.write)
        dataBytes = dataStream.getvalue()
        return dataBytes
    
    def ftpFileToZip(self, ftpFilePath):
        zipBytes = self.ftpFileToBytes(ftpFilePath)
        Zip = self.ZIP(zipBytes)
        return Zip

    def bytesToFtpFile(self, fileBytes, ftpFilePath):
        dataStream = self.StringIO(fileBytes)
        self.ftpConnection.storbinary("STOR " + ftpFilePath, dataStream)

    """
    Workflow 02's methods
    """

    def bytesToSparkRdd(self, dataBytes, lineDelimiter='\n'):
        dataBytes = self._dropMsWindowsLineDelimiter(dataBytes, lineDelimiter)
        # Upload the bytes into a splitted Text RDD
        rdd = self.sparkContext.parallelize(dataBytes.split(lineDelimiter))
        # Return the RDD
        return rdd

    def bytesToSparkBinaryRdd(self, dataBytes):
        # Upload the bytes into a one block binary RDD
        emptyRdd = self.sparkContext.parallelize(["noData"])
        rdd = emptyRdd.map(lambda noData: dataBytes)
        # Return the RDD
        return rdd

    def csvBytesToSparkDf(self, dataBytes, isHeader=True, columns=None, columnDelimiter=';', lineDelimiter='\n'):
        dataBytes = self._dropMsWindowsLineDelimiter(dataBytes, lineDelimiter)
        rdd = self.bytesToSparkRdd(dataBytes, lineDelimiter)
        rdd = self._rddExcludeBlankLines(rdd)
        if (isHeader == True):
            if (columns == None): columns = self._rddGetHeader(rdd, columnDelimiter)
            rdd = self._rddExcludeHeader(rdd)
        if (columns != None):
            rdd = self._rddExcludeLinesAsHeader(rdd, columns, columnDelimiter, sensitiveCase=False)
            rdd = self._rddExcludeLinesWithWrongNumberOfColumns(rdd, columns, columnDelimiter)
        rdd = rdd.map(lambda line: line.split(columnDelimiter))
        if (columns != None): df = rdd.toDF(columns)
        else: df = rdd.toDF()
        return df

    def sparkDfToCsvBytes(self, df, isHeader=True, columnDelimiter=',', lineDelimiter='\n'):
        def _getHeader(row, columnDelimiter):
            header = None
            cols = str(row)[4:-1].split(',')
            for col in cols:
                name = col[0:col.find('=')].strip()
                if (header == None): header = name
                else: header += columnDelimiter + name
            return header
        rows = df.collect()
        header = _getHeader(rows[0], columnDelimiter)
        dataBytes = None
        for row in rows:
            rowStr = None
            for colVal in row:
                if (rowStr == None): rowStr = str(colVal)
                else: rowStr += columnDelimiter + str(colVal)
            if (dataBytes == None):
                if (isHeader == True): dataBytes = header + lineDelimiter + rowStr
                else: dataBytes = rowStr
            else: dataBytes += lineDelimiter + rowStr
        return dataBytes

    """
    Workflow 03's methods
    """

    def localFileToBytes(self, localFilePath):
        localFilePath = _cleanLocalPath(localFilePath)    
        with open(localFilePath, 'r') as content_file: dataBytes = content_file.read()
        return dataBytes

    #def bytesToLocalFile(...)

    """
    Workflow 04's methods
    """

    def ftpFileToSparkRdd(self, ftpFilePath, lineDelimiter='\n'):
        # Download the local file bytes into the memory (from the Spark driver side)
        dataBytes = self.ftpFileToBytes(self, ftpFilePath)
        # Upload the bytes to a splitted Text RDD
        rdd = self.bytesToSparkRdd(dataBytes, lineDelimiter)
        return rdd

    def ftpFileToSparkBinaryRdd(self, ftpFilePath):
        # Download the local file bytes into the memory (from the Spark driver side)
        dataBytes = self.ftpFileToBytes(self, ftpFilePath)
        # Upload the bytes to a one block binary RDD
        rdd = self.bytesToSparkBinaryRdd(dataBytes)
        return rdd

    def ftpCsvFileToSparkDf(self, ftpFilePath, isHeader=True, columns=None, columnDelimiter=';', lineDelimiter='\n'):
        dataBytes = self.ftpFileToBytes(ftpFilePath)
        df = self.csvBytesToSparkDf(dataBytes, isHeader, columns, columnDelimiter, lineDelimiter)
        return df

    #def sparkRddToFtpFile(...)

    def sparkDfToFtpCsvFile(self, df, ftpFilePath, isHeader=True, columnDelimiter=';', lineDelimiter='\n'):
        dataBytes = self.sparkDfToCsvBytes(df, isHeader, columnDelimiter, lineDelimiter)
        self.bytesToFtpFile(dataBytes, ftpFilePath)

    """
    Workflow 05's methods
    """

    def localFileToSparkRdd(self, localFilePath, lineDelimiter='\n'):
        # Download the local file bytes into the memory (from the Spark driver side)
        dataBytes = self.localFileToBytes(localFilePath)
        # Upload the bytes to a splitted Text RDD
        rdd = self.bytesToSparkRdd(dataBytes, lineDelimiter)
        return rdd

    def localFileToSparkBinaryRdd(self, localFilePath):
        # Download the local file bytes into the memory (from the Spark driver side)
        dataBytes = self.localFileToBytes(localFilePath)
        # Upload the bytes to a one block binary RDD
        rdd = self.bytesToSparkBinaryRdd(dataBytes)
        return rdd

    def localCsvFileToSparkDf(self, localFilePath, isHeader=True, columns=None, columnDelimiter=';', lineDelimiter='\n'):
        dataBytes = self.localFileToBytes(localFilePath)
        df = self.csvBytesToSparkDf(dataBytes, isHeader, columns, columnDelimiter, lineDelimiter)
        return df

    #def sparkRddToLocalFile(...)

    #def sparkDfToLocalCsvFile(...)

    """
    Workflow 06's methods
    """
    
    #def hdfsToBytes(...)

    #def bytesToHdfs(...)

    """
    Workflow 07's methods
    """
    
    #def ftpFileToLocalFile(...)
    
    #def localFileToFtpFile(...)

    """
    Workflow 08's methods
    """

    #def localFileToHdfs(...)
    
    #def hdfsToLocalFile(...)

    """
    Workflow 09's methods
    """

    #def ftpFileToHdfs(...)
    
    #def hdfsToFtpFile(...)

    # =========================================
    # =            Private methods            =
    # =========================================

    def _cleanLocalPath(localPath):
        if (localPath != None):
            localPath = localPath.strip()
            if (localPath.startswith("file://")): localPath = localPath[7:]
            if (localPath.endswith("/")): localPath = localPath[:-1]
        return localPath

    def _cleanFtpFolder(self, ftpFolder):
        if (ftpFolder != None):
            ftpFolder = ftpFolder.strip()
            if (len(ftpFolder) > 1 and ftpFolder.endswith('/')): ftpFolder = ftpFolder[0: -1]
        if (ftpFolder == None):
            raise Exception("The FTP folder is null, so it must starts with '/' !")
        if (not ftpFolder.startswith("/")):
            raise Exception("The FTP folder '" + ftpFolder + "' must be an absolute path, so it must starts with '/' !")
        return ftpFolder

    def _scanFtpFolders(self, ftpConnection, ftpParentFolder="/", recursive=False, startsWith=None, contains=None, endsWith=None):
        ftpParentFolder = self._cleanFtpFolder(ftpParentFolder)
        if (isinstance(recursive, bool) == False): recursive = False
        ftpSubFolders = []
        self.__scanFtpFolders(ftpConnection, ftpParentFolder, ftpSubFolders, recursive, startsWith, contains, endsWith)
        if (ftpSubFolders == None or len(ftpSubFolders) == 0): ftpSubFolders = None
        return ftpSubFolders
    def __scanFtpFolders(self, ftpConnection, ftpParentFolder, ftpSubFolders, recursive=False, startsWith=None, contains=None, endsWith=None):
        ls = []
        ftpConnection.cwd(ftpParentFolder)
        ftpConnection.retrlines("LIST", ls.append)
        if (len(ls) > 0):
            for line in ls:
                words = line.split()
                for index, word in enumerate(words): words[index] = words[index].strip()
                if (words[0].startswith('d')):
                    aFtpRelativeFolder = words[8]
                    if (ftpParentFolder == '/'): aFtpAbsoluteFolder = "/" + aFtpRelativeFolder
                    else: aFtpAbsoluteFolder = ftpParentFolder + "/" + aFtpRelativeFolder
                    aFtpAbsoluteFolder = aFtpAbsoluteFolder.replace("//", "/")
                    if (self._filterPath(aFtpRelativeFolder, startsWith, contains, endsWith) != None):
                        ftpSubFolders.append(aFtpAbsoluteFolder)
                    if (recursive):
                        self.__scanFtpFolders(ftpConnection, aFtpAbsoluteFolder, ftpSubFolders, recursive, startsWith, contains, endsWith)

    def _scanFtpFiles(self, ftpConnection, ftpParentFolder="/", recursive=False, startsWith=None, contains=None, endsWith=None):
        ftpParentFolder = self._cleanFtpFolder(ftpParentFolder)
        if (isinstance(recursive, bool) == False): recursive = False
        ftpFolders = []
        ftpFolders.append(ftpParentFolder)
        if (recursive == True):
            ftpSubFolders = self._scanFtpFolders(ftpConnection, ftpParentFolder, recursive)
            for folder in ftpSubFolders: ftpFolders.append(folder)
        ftpFiles = []
        for ftpFolder in ftpFolders:
            ftpConnection.cwd(ftpFolder)
            ls = []
            ftpConnection.retrlines("LIST", ls.append)
            for line in ls:
                words = line.split()
                for index, word in enumerate(words): words[index] = words[index].strip()
                if (not words[0].startswith('d')):
                    aFile = words[8]
                    aFile = self._filterPath(aFile, startsWith, contains, endsWith)
                    if (aFile != None):
                        absoluteFile = ftpFolder + "/" + aFile
                        absoluteFile = absoluteFile.replace("//", "/")
                        ftpFiles.append(absoluteFile)
        if (ftpFiles == None or len(ftpFiles) == 0): ftpFiles = None
        return ftpFiles

    def _filterPath(self, aPath, startsWith=None, contains=None, endsWith=None):
        if (aPath == None or len(aPath) == 0): return None
        # Starts with
        if (startsWith != None): startsWith = startsWith.strip()
        if (startsWith == ""): startsWith = None
        # Contains
        if (contains != None): contains = contains.strip()
        if (contains == ""): contains = None
        # Ends with
        if (endsWith != None): endsWith = endsWith.strip()
        if (endsWith == ""): endsWith = None
        # Filtering
        filteredPath = aPath
        if (startsWith != None or contains != None or endsWith != None):
            match = True
            if (startsWith != None and not aPath.startswith(startsWith)): match = False
            if (contains != None and not (contains in aPath)): match = False
            if (endsWith != None and not aPath.endswith(endsWith)): match = False
            if (match == True): filteredPath = aPath
            else: filteredPath = None
        return filteredPath

    def _dropMsWindowsLineDelimiter(self, dataBytes, lineDelimiter):
        if (lineDelimiter == '\n'): dataBytes = dataBytes.replace('\r', '')
        if (lineDelimiter == '\r'): dataBytes = dataBytes.replace('\n', '')
        return dataBytes

    def _cleanHeader(self, columns):
        for i, column in enumerate(columns):
            columns[i] = column.strip()
            if (column.startswith('"') and column.endswith('"')): columns[i] = column[1:-1].strip()
        return columns

    def _rddExcludeBlankLines(self, rdd):
        rdd = rdd.filter(lambda line: line.strip() != "")
        return rdd

    def _rddGetHeader(self, rdd, columnDelimiter):
        columns = rdd.first().split(columnDelimiter)
        columns = self._cleanHeader(columns)
        return columns

    def _rddExcludeHeader(self, rdd):
        header = rdd.first()
        rdd = rdd.filter(lambda line: line != header)
        return rdd

    def _rddExcludeLinesAsHeader(self, rdd, columns, columnDelimiter=';', sensitiveCase=False):
        def _lineIsNotHeader(line, columns, columnDelimiter, sensitiveCase=False):
            lineIsHeader = True
            lineCols = line.split(columnDelimiter)
            for i, column in enumerate(lineCols):
                lineCols[i] = column.strip()
                if (column.startswith('"') and column.endswith('"')): lineCols[i] = lineCols[1:-1].strip()
            if (len(columns) == len(lineCols)):
                for i, col in enumerate(columns):
                    if ((sensitiveCase == True and col != lineCols[i]) or
                        (sensitiveCase == False and col.lower() != lineCols[i].lower())):
                        lineIsHeader = False
                        break
            else: lineIsHeader = False
            return not lineIsHeader
        rdd = rdd.filter(lambda line: _lineIsNotHeader(line, columns, columnDelimiter, sensitiveCase))
        return rdd

    def _rddExcludeLinesWithWrongNumberOfColumns(self, rdd, columns, columnDelimiter=';'):
        def _isLineWithRightNumberOfColumns(line, columns, columnDelimiter):
            numberOfDelimiters = line.count(columnDelimiter)
            if ((1+numberOfDelimiters) == len(columns)): result = True
            else: result = False
            return result
        rdd = rdd.filter(lambda line: _isLineWithRightNumberOfColumns(line, columns, columnDelimiter))
        return rdd
