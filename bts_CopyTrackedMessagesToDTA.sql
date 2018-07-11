CREATE PROCEDURE [dbo].[bts_CopyTrackedMessagesToDTA]  
@dtaDbServer sysname,  
@dtaDbName sysname  
AS  
set transaction isolation level read committed  
set nocount on  
set deadlock_priority low  
declare @count int, @nTotalNumParts int, @fAppLockTaken bit,  
 @uidMessageID uniqueidentifier,  
 @tnActiveTable tinyint,  
 @tnActiveTrackingSpool tinyint, @retVal int,  
 @dtBeginTime datetime, @dtEndTime datetime,  
  
 @timeSpan int  
    
SELECT @dtBeginTime=GETDATE()  
  
    
CREATE TABLE #TrkMsgRefIDs (uidMessageID uniqueidentifier NOT NULL)  
CREATE CLUSTERED INDEX [CIX_TrkMsgRefIDs] ON [#TrkMsgRefIDs](uidMessageID)  
  
CREATE TABLE #TrkMsgNewPartIDs (uidMessageID uniqueidentifier NOT NULL, uidOldPartID uniqueidentifier NOT NULL, nvcPartName nvarchar(256) NOT NULL, uidNewPartID uniqueidentifier NOT NULL DEFAULT NewID() )  
CREATE CLUSTERED INDEX [CIX_TrkMsgNewPartIDs] ON [#TrkMsgNewPartIDs](uidMessageID, uidOldPartID)  
  
  
declare @localized_string_CopyTrackedMessages_Invalid_Params nvarchar(128)  
set @localized_string_CopyTrackedMessages_Invalid_Params = N'Non null values must be provided for the tracking server and database names.'  
  
  
if ( (@dtaDbServer IS NULL) OR (@dtaDbName IS NULL) )  
BEGIN  
 RAISERROR(@localized_string_CopyTrackedMessages_Invalid_Params, 16, 1)  
 return  
END  
  
set @uidMessageID = NULL  
SELECT TOP 1 @uidMessageID = uidMessageID FROM dbo.TrackingMessageReferences WITH (ROWLOCK) OPTION (KEEPFIXED PLAN)  
while (@uidMessageID IS NOT NULL)  
BEGIN  
  
 set @fAppLockTaken = 0  
 --let's see how many messages there are to track. If there are less than 50, we need to lock the references table to   
 --make sure that no more come in while we are processing. If they did, we would only partially track messages and could  
 --delete a reference to a message which was meant to be tracked even though we did not track it.  
 SELECT TOP 50 @count = COUNT(*) FROM dbo.TrackingMessageReferences WITH (ROWLOCK)  
 if (@count < 50)  
 BEGIN  
  exec  @retVal = sp_getapplock 'TrkMsgRefs', 'Exclusive', 'Session'  
  IF (@retVal < 0 ) -- Lock Not granted  
  BEGIN  
   RAISERROR('Unable to acquire applock on TrackingMessageReferences', 16, 1)  
   return  
  END  
  set @fAppLockTaken = 1  
 END  
   
 SELECT @nTotalNumParts = SUM(q.nNumParts)  
 FROM   
 (     
            SELECT TOP 50 s.nNumParts, 1 as idx  
   FROM (SELECT TOP 50 uidMessageID FROM dbo.TrackingMessageReferences WITH (ROWLOCK INDEX(IX_TrackingMessageReferences)) ORDER BY nID ASC) as t  
            JOIN dbo.Spool s WITH(ROWLOCK INDEX(IX_Spool))   ON s.uidMessageID = t.uidMessageID  
 ) as q  
  GROUP BY q.idx  
  
 TRUNCATE TABLE #TrkMsgNewPartIDs  
 if (@nTotalNumParts > 300)  
 BEGIN  
  INSERT INTO #TrkMsgNewPartIDs (uidMessageID, uidOldPartID, nvcPartName)  
  SELECT mp.uidMessageID, mp.uidPartID, mp.nvcPartName  
  FROM  (SELECT TOP 50 uidMessageID FROM dbo.TrackingMessageReferences WITH (ROWLOCK INDEX(IX_TrackingMessageReferences)) ORDER BY nID ASC) as t  
  JOIN dbo.MessageParts mp ON t.uidMessageID = mp.uidMessageID  
 END  
 ELSE  
 BEGIN  
  INSERT INTO #TrkMsgNewPartIDs (uidMessageID, uidOldPartID, nvcPartName)  
  SELECT TOP 300 mp.uidMessageID, mp.uidPartID, mp.nvcPartName  
  FROM  (SELECT TOP 50 uidMessageID FROM dbo.TrackingMessageReferences WITH (ROWLOCK INDEX(IX_TrackingMessageReferences)) ORDER BY nID ASC) as t  
  JOIN dbo.MessageParts mp ON t.uidMessageID = mp.uidMessageID  
 END  
  
 exec ('INSERT INTO [' + @dtaDbServer + '].[' + @dtaDbName + '].[dbo].[Tracking_Parts1](uidMessageID, nvcPartName, uidPartID,  uidOldPartID, nNumFragments, imgPart, imgPropBag)  
 SELECT TOP ' + @nTotalNumParts + ' mp.uidMessageID, mp.nvcPartName, mp.uidNewPartID,  mp.uidOldPartID, p.nNumFragments, p.imgPart, p.imgPropBag  
 FROM (SELECT TOP 50 uidMessageID, nID FROM dbo.TrackingMessageReferences WITH (ROWLOCK INDEX(IX_TrackingMessageReferences)) ORDER BY nID ASC) as  t  
 INNER LOOP JOIN #TrkMsgNewPartIDs mp WITH (ROWLOCK) ON t.uidMessageID = mp.uidMessageID  
 INNER LOOP JOIN dbo.Parts p WITH (ROWLOCK INDEX(IX_Parts)) ON mp.uidOldPartID = p.uidPartID  
 OPTION (KEEPFIXED PLAN)')  
  
 IF @@ERROR <>0  
 BEGIN  
  if (@fAppLockTaken = 1)  
  BEGIN  
   exec sp_releaseapplock 'TrkMsgRefs', 'Session'  
  END  
  RAISERROR ('An error occurred while inserting data in the Tracking_Parts1 table',16,1)  
  return  
 END  
  
 IF EXISTS (SELECT TOP 1 p.nNumFragments   
        FROM  (SELECT TOP 50 uidMessageID, nID FROM dbo.TrackingMessageReferences WITH (ROWLOCK INDEX(IX_TrackingMessageReferences)) ORDER BY nID ASC) as  t  
    INNER LOOP JOIN dbo.MessageParts mp WITH (ROWLOCK INDEX(CIX_MessageParts)) ON t.uidMessageID = mp.uidMessageID  
    INNER LOOP JOIN dbo.Parts p WITH (ROWLOCK INDEX(IX_Parts)) ON mp.uidPartID = p.uidPartID AND p.nNumFragments > 1)  
 BEGIN    
  exec ('INSERT INTO [' + @dtaDbServer + '].[' + @dtaDbName + '].[dbo].[Tracking_Fragments1](uidPartID, nFragmentNumber, nOffsetStart, nOffsetEnd, imgFrag)  
  SELECT mp.uidNewPartID, f.nFragmentNumber, f.nOffsetStart, f.nOffsetEnd, f.imgFrag  
  FROM  (SELECT TOP 50 uidMessageID, nID FROM dbo.TrackingMessageReferences WITH (ROWLOCK INDEX(IX_TrackingMessageReferences)) ORDER BY nID ASC) as  t  
  INNER LOOP JOIN #TrkMsgNewPartIDs mp WITH (ROWLOCK) ON t.uidMessageID = mp.uidMessageID  
  INNER LOOP JOIN dbo.Fragments f WITH(ROWLOCK INDEX(IX_Fragments)) ON mp.uidOldPartID = f.uidPartID  
  OPTION (KEEPFIXED PLAN)')  
  
  IF @@ERROR <>0  
  BEGIN  
   if (@fAppLockTaken = 1)  
   BEGIN  
    exec sp_releaseapplock 'TrkMsgRefs', 'Session'  
   END  
   RAISERROR ('An error occurred while inserting data in the Tracking_Fragments1 table',16,1)  
   return  
  END  
 END  
  
 --copy the spool row last so that its datetime can be used for the whole message  
 exec ('INSERT INTO [' + @dtaDbServer + '].[' + @dtaDbName + '].[dbo].[Tracking_Spool1](uidMsgID, UserName, dtTimeStamp, dtExpiration, nNumParts, uidBodyPartID, imgContext)  
 SELECT s.uidMessageID, s.UserName, s.dtTimeStamp, s.dtExpiration, s.nNumParts, s.uidBodyPartID, s.imgContext  
 FROM (SELECT TOP 50 uidMessageID, nID FROM dbo.TrackingMessageReferences WITH (ROWLOCK INDEX(IX_TrackingMessageReferences)) ORDER BY nID ASC) as t  
 INNER LOOP JOIN dbo.Spool s WITH(ROWLOCK INDEX(IX_Spool))  
 ON s.uidMessageID = t.uidMessageID  
 OPTION (KEEPFIXED PLAN)')  
   
 IF @@ERROR <>0  
 BEGIN  
  if (@fAppLockTaken = 1)  
  BEGIN  
   exec sp_releaseapplock 'TrkMsgRefs', 'Session'  
  END  
  RAISERROR ('An error occurred while inserting data in the Spool table',16,1)  
  return  
 END  
  
 --if we took the applock I don't want to hold it through the transaction managing the refcounts. If we did that,   
 --I might get some wierd deadlock between the refcountlog and trackmessagereferences applocks  
 if (@fAppLockTaken = 1)  
 BEGIN  
  INSERT INTO [#TrkMsgRefIDs]  
  SELECT TOP 50 uidMessageID FROM dbo.TrackingMessageReferences WITH (ROWLOCK INDEX(IX_TrackingMessageReferences)) ORDER BY nID ASC  
     
  exec sp_releaseapplock 'TrkMsgRefs', 'Session'  
 END  
   
    BEGIN TRANSACTION  
 --lets take a lock for checking the refcount log. We use an applock so that we can explicitly release the lock  
 --normal locks would require us to wait for the transaction to complete, but we don't really need to wait that long  
 exec  @retVal = sp_getapplock 'MessageRefCountLog', 'Shared', 'Transaction'  
 IF (@retVal < 0 ) -- Lock Not granted  
 BEGIN  
  RAISERROR('Unable to acquire applock on MessageRefCountLog', 16, 1)  
  return  
 END  
 SELECT TOP 1 @tnActiveTable = tnActiveTable FROM dbo.ActiveRefCountLog WITH (ROWLOCK) WHERE fType = 1 OPTION (KEEPFIXED PLAN)  
   
 if (@fAppLockTaken = 0)  
 BEGIN  
  if (@tnActiveTable = 1)  
  BEGIN  
   INSERT INTO dbo.MessageRefCountLog1 WITH (ROWLOCK) (uidMessageID, tnQueueID, snRefCount)  
   SELECT TOP 50 uidMessageID, 4, -1 FROM dbo.TrackingMessageReferences WITH (ROWLOCK INDEX(IX_TrackingMessageReferences)) ORDER BY nID ASC  
  END  
  else  
  BEGIN  
   INSERT INTO dbo.MessageRefCountLog2 WITH (ROWLOCK) (uidMessageID, tnQueueID, snRefCount)  
   SELECT TOP 50 uidMessageID, 4, -1 FROM dbo.TrackingMessageReferences WITH (ROWLOCK INDEX(IX_TrackingMessageReferences)) ORDER BY nID ASC  
     
  END  
 END  
 else  
 BEGIN  
  if (@tnActiveTable = 1)  
  BEGIN  
   INSERT INTO dbo.MessageRefCountLog1 WITH (ROWLOCK) (uidMessageID, tnQueueID, snRefCount)  
   SELECT uidMessageID, 4, -1 FROM #TrkMsgRefIDs  
  END  
  else  
  BEGIN  
   INSERT INTO dbo.MessageRefCountLog2 WITH (ROWLOCK) (uidMessageID, tnQueueID, snRefCount)  
   SELECT uidMessageID, 4, -1 FROM #TrkMsgRefIDs  
  END  
 END  
   
 exec sp_releaseapplock 'MessageRefCountLog', 'Transaction'  
 --Lets remove anything that has already been tracked. Make sure to also remove from trackingmessageref table  
 if (@fAppLockTaken = 0)  
 BEGIN  
  DELETE FROM dbo.TrackingMessageReferences  
  FROM  (SELECT TOP 50 uidMessageID, nID FROM dbo.TrackingMessageReferences WITH (ROWLOCK INDEX(IX_TrackingMessageReferences)) ORDER BY nID ASC) as t  
  JOIN dbo.TrackingMessageReferences as tmr WITH (ROWLOCK INDEX(CIX_TrackingMessageReferences))  
  ON tmr.uidMessageID = t.uidMessageID  
  OPTION (KEEPFIXED PLAN, FORCE ORDER)  
 END  
 ELSE  
 BEGIN  
  DELETE FROM dbo.TrackingMessageReferences  
  FROM  #TrkMsgRefIDs t  
  JOIN dbo.TrackingMessageReferences as tmr WITH (ROWLOCK INDEX(CIX_TrackingMessageReferences))  
  ON tmr.uidMessageID = t.uidMessageID  
  OPTION (KEEPFIXED PLAN, FORCE ORDER)  
 END  
   
    COMMIT TRANSACTION  
    
 SET @uidMessageID = NULL  
 SELECT TOP 1 @uidMessageID = uidMessageID FROM dbo.TrackingMessageReferences WITH (ROWLOCK) OPTION (KEEPFIXED PLAN)  
  
 TRUNCATE TABLE #TrkMsgRefIDs   
END  
  
DROP TABLE #TrkMsgRefIDs   
DROP TABLE #TrkMsgNewPartIDs  
  
-- Update the duration in JobData so that perf counters stored procs can get it efficiently   
SELECT @dtEndTime=GETDATE()  
  
set @timeSpan=DATEDIFF(ss, @dtBeginTime, @dtEndTime)  
  
--This proc is called by TrackedMessages_Copy job  
declare @jobName sysname  
select @jobName = 'TrackedMessages_Copy_'+DB_NAME()  
  
UPDATE JobData SET Duration = @timeSpan   
WHERE JobName = @jobName