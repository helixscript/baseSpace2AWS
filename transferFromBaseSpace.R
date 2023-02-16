library(dplyr)
library(lubridate)

# Requirements:
# 1. BaseSpace CLI needs to be installed and configured.
# 2. Communication between the AWS instance running this script and target buckets needs to be seamless (no prompting).
# 3. Targeted bucket requires existing manifest/ and data/ folders and the data folder is expected to have 
#    year folders already created, eg. 2021/ 2022/ 2023/ since programatic folder creation is often restricted.
#
# Retrieve all records in S3 manifest:  
# aws s3 cp s3://bushman-sequencing-data/Illumina-paired-end/manifest/ ./ --recursive

bs  <- '/home/ubuntu/bin/bs'
tmp <- '/home/ubuntu/data/baseSpace2AWS/tmp'
logFile <- '/home/ubuntu/data/baseSpace2AWS/log'
bucket  <- 'bushman-sequencing-data/Illumina-paired-end'
localManifestDir <- '/var/www/html/data/sequencingSampleData'
allowed_statuses <- c('Analyzing', 'Complete', 'Needs Attention', 'Pending Analysis')

# Last path element function.
lpe <- function(x){ 
  o <- unlist(strsplit(x, '/'))
  o[length(o)]
}

# Build a list of runs already archived on AWS.
remoteListing <- function(){
  bind_rows(lapply(system(paste0('aws s3 ls s3://', bucket, '/data/ --recursive'), intern = TRUE), function(x){
       o <- unlist(strsplit(x, '\\s+'))
       if(length(o) != 4) return(tibble())
       if(substr(o[4], nchar(o[4]), nchar(o[4])) == '/') return(tibble())
       tibble(date = o[1], time = o[2], size = o[3], file = lpe(o[4]))
     }))
}

d <- remoteListing()

# Retrieve and parse a table of runs from BaseSpace.
r <- bind_rows(lapply(strsplit(system(paste0(bs, ' list runs'), intern = TRUE), '\n'), function(x){
       s <- unlist(strsplit(x, '\\s*\\|\\s*'))
       
       if(length(s) >= 5 & nchar(s[2]) >= 5){
         if(nchar(s[2]) > 5 & nchar(s[3]) > 5 & nchar(s[5]) > 5){
           return(tibble(runName = s[2], runID = s[3], date = ymd(unlist(strsplit(s[2], '_'))[1]), status = s[5]))
         } else {
           return(tibble())
         }
       } else {
         return(tibble())
       }
     })) %>% filter(status %in% allowed_statuses) %>% arrange(date)

if(nrow(r) == 0) q()


# Error reporter function.
catchError <- function(name, error, logFile){
  write(paste(date(), paste(name, 'failed to transfer!', error)), file = logFile, append = TRUE)
  q(save = "no", status = 1)
}

# Download runs that have not been transferred to AWS, compress them, and transfer.
# SampleSheets are archived in both the local and remote manifest/ directories.

write(date(), file = logFile, append = FALSE)
invisible(lapply(1:nrow(r), function(x){
       x <- r[x,]
       
       if(! paste0(x$runName, '.tar.gz') %in% d$file){
         remoteDataDir <- paste0('20', substr(x$runName, 1, 2)) # Limited to years > 1999
      
         # Download run from BaseSpace.
         o <- system(paste0(bs, ' download run -q --id ', x$runID, ' -o ', file.path(tmp, x$runName)), intern = FALSE)
         if(o != 0) catchError(x$runName, 'Error 1', logFile)
         
         # Remove fastq files that may have been generated with BaseSpace.
         f <- list.files(file.path(tmp, x$runName, 'Data', 'Intensities', 'BaseCalls'), pattern = '.fastq', full.names = TRUE)
         if(length(f) > 0) invisible(file.remove(f))
         
         # Check for SampleSheet.csv file.
         if(! file.exists(file.path(tmp, x$runName, 'SampleSheet.csv'))) catchError(x$runName, 'Error 2', logFile)
         
         # Copy SampleSheet.csv to local manifest archive.
         o <- system(paste('cp', file.path(tmp, x$runName, 'SampleSheet.csv'), file.path(localManifestDir, paste0(x$runName, '.csv'))), intern = FALSE)
         if(o != 0) catchError(x$runName, 'Error 3', logFile)
         
         # Create tar ball.
         o <- system(paste0('tar cfz ', file.path(tmp, x$runName), '.tar.gz -C ', tmp, ' ', x$runName), intern = FALSE)
         if(o != 0) catchError(x$runName, 'Error 4', logFile)
         
         # Copy tar ball to AWS.
         o <- system(paste0('aws s3 cp ',  file.path(tmp, x$runName), '.tar.gz s3://', bucket, '/data/', remoteDataDir, '/'), intern = FALSE)
         if(o != 0) catchError(x$runName, 'Error 5', logFile)
         
         # Copy SampleSheet to AWS.
         o <- system(paste0('aws s3 cp ',  file.path(localManifestDir, paste0(x$runName, '.csv')), ' s3://', bucket, '/manifest/'), intern = FALSE)
         if(o != 0) catchError(x$runName, 'Error 6', logFile)
         
         # Check for existence of transfer. Checksum validation should be handled by AWS.
         a <- system(paste0('aws s3 ls s3://', bucket, '/data/', remoteDataDir, '/', paste0(x$runName, '.tar.gz')), intern = TRUE)
         b <- system(paste0('aws s3 ls s3://', bucket, '/manifest/', paste0(x$runName, '.csv')), intern = TRUE)
         
         if(length(a) > 0 & length(b) > 0){
           write(paste(date(), paste(x$runName, 'transfered.')), file = logFile, append = TRUE)
         } else {
           write(paste(date(), paste(x$runName, 'failed to transfer!')), file = logFile, append = TRUE)
           q(save = "no", status = 1)
         }
        
         system(paste0('rm -rf ', file.path(tmp, x$runName), '*'))
       }
}))
  
q(save = "no", status = 0)
