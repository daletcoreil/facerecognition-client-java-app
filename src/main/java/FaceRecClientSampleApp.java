import com.amazonaws.HttpMethod;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.dalet.mediator.facerecognition.ApiClient;
import com.dalet.mediator.facerecognition.ApiException;
import com.dalet.mediator.facerecognition.Configuration;
import com.dalet.mediator.facerecognition.api.AuthApi;
import com.dalet.mediator.facerecognition.api.JobsApi;
import com.dalet.mediator.facerecognition.api.FaceRecognitionApi;
import com.dalet.mediator.facerecognition.auth.ApiKeyAuth;
import com.dalet.mediator.facerecognition.model.*;
import org.json.JSONObject;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class FaceRecClientSampleApp {

    /**
     * Mediator Client Id
     */
    private final String    client_id;

    /**
     * Mediator Client Secret
     */
    private final String    client_secret;

    /**
     * Project Service Id
     */
    private final String    project_service_id;


    /**
     * S3 client bucket name.
     */
    private final String    bucketName;
    private final String    inputKey;

    private final String    folderPath;
    private final String    inputFile;
		
    private final Integer   file_duration   = 30;

    private final String    basePath;
    private final String    region;
    private final String    language = "english";

    private final String    aws_access_key_id;
    private final String    aws_secret_access_key;
    private final String    aws_session_token;

    private final String    inputImageKey;
    private final String    inputImageFile;
	
		
    private ApiClient apiClient;
    private AmazonS3 s3Client;
    private JobsApi jobsApi;
    private FaceRecognitionApi faceRecognitionApi;

    public static void main(String[] args) {
        try {
            FaceRecClientSampleApp impl = new FaceRecClientSampleApp(args);
            impl.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public FaceRecClientSampleApp(String[] args) throws Exception{
        String data = new String(Files.readAllBytes(Paths.get(System.getenv("APP_CONFIG_FILE"))));
        if(data == null) {
            throw new ApiException("Configuration file 'app-config.json' is not found in " + System.getenv("APP_CONFIG_FILE"));
        }
        JSONObject config = new JSONObject(data);
        client_id = config.getString("clientKey");
        client_secret = config.getString("clientSecret");
        project_service_id = config.getString("projectServiceId");
        bucketName = config.getString("bucketName");
		region = config.has("bucketRegion") ? config.getString("bucketRegion") : "us-east-1";	
		
        inputKey = config.getString("inputFile");
        folderPath = config.getString("localPath");
		inputFile       =  folderPath + inputKey;
		
        inputImageKey = config.getString("inputImage");
		inputImageFile       =  folderPath + inputImageKey;
		
        basePath = config.has("host") ? config.getString("host") : null;
		aws_access_key_id = config.has("aws_access_key_id") ? config.getString("aws_access_key_id") : null;
		aws_secret_access_key = config.has("aws_secret_access_key") ? config.getString("aws_secret_access_key") : null;
		aws_session_token = config.has("aws_session_token") ? config.getString("aws_session_token") : null;	
		
		if(aws_secret_access_key == null || aws_access_key_id == null) {
			throw new ApiException("AWS credentials are not defined in app-config.json file");
		}
    }

    private void run() throws ApiException, InterruptedException {
        /// credentials
        initAmazonS3Client();
        /// upload media ///////////
        uploadMediaToS3();
        // refresh auth token //////
        initializeDaletMediatorClient();
        // prepare job //////////////
        JobMediatorInput jobMediatorInput = prepareExtractFacesMedatorJob();
        // post job ////////
        MediatorJob job = postJob(jobMediatorInput);
        // validate job ////
        validateJobResponse(job);
        // wait job to complete ////
        job = waitForComplete(job.getId());
		////////////////////
		System.out.println("Finished extract faces job");
		
		// get collectionId argument from jobId
		String faceExtractionId = ((ExtractFacesOutput)job.getJobOutput()).getFaceExtractionId();

        System.out.println("Querying Face API for extraction info: " + faceExtractionId);
        FaceExtractionCollection faceExtraction = faceRecognitionApi.getFaceExtractionCollection(project_service_id, faceExtractionId);

        String faceId = faceExtraction.getFaceIds().get(0);
        System.out.println("Querying Face API for first face info: " + faceId);
        Face face = faceRecognitionApi.getFace(project_service_id, faceId);

       // prepare job //////////////
        jobMediatorInput = prepareClusterFacesMedatorJob(faceExtractionId);
        // post job ////////
        job = postJob(jobMediatorInput);
        // validate job ////
        validateJobResponse(job);
        // wait job to complete ////
        job = waitForComplete(job.getId());
		String clusterCollectionId = ((ClusterFacesOutput)job.getJobOutput()).getClusterCollectionId();
		System.out.println("Finished cluster faces job. Return clusterCollectionId: " + clusterCollectionId);

        System.out.println("Querying Face API for extraction info: " + clusterCollectionId);
        ClusterCollection clusterCollection = faceRecognitionApi.getClusterCollection(project_service_id, clusterCollectionId);

		uploadImageToS3();
		jobMediatorInput = prepareSearchFacesMedatorJob(clusterCollectionId);
        // post job ////////
        job = postJob(jobMediatorInput);
        // validate job ////
        validateJobResponse(job);
        // wait job to complete ////
        job = waitForComplete(job.getId());
		////////////////////
		System.out.println("Finished search faces job");	
    }

    private MediatorJob waitForComplete(String jobId) throws ApiException {
        /// sleep between getjob requests - 30 seconds
        long sleepTime = 1000L * 30;
        do {
            System.out.println("Checking job status ...");
            MediatorJob mediatorJob = jobsApi.getJobById(jobId);
            // Validate job response for error
            validateJobResponse(jobsApi.getJobById(jobId));
            // check job for completed ///
            if (mediatorJob.getStatus().getStatus().equals(JobMediatorStatus.StatusEnum.COMPLETED)) {
				System.out.println("Received job COMPLETED status!");
                return mediatorJob;
            }
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ApiException("Got interupted execption event. jobId: " + jobId);
            }
        }
        while (true);
    }

    protected void validateJobResponse(MediatorJob mediatorJob) throws ApiException {
        if (mediatorJob.getStatus().getStatus().equals(JobMediatorStatus.StatusEnum.FAILED)) {
            throw new ApiException("Cortex failed to perform the job. StatusMessage: [" + mediatorJob.getStatus().getStatusMessage() + "]");
        }
    }



    private MediatorJob postJob(JobMediatorInput jobMediatorInput) throws ApiException {
        System.out.println("Posting mediator job ...");
        return jobsApi.createJob(jobMediatorInput);
    }

    private void uploadMediaToS3() throws InterruptedException {
        System.out.println("Uploading media to S3 bucket ...");
        TransferManager tm = TransferManagerBuilder.standard().withS3Client(s3Client).build();
        Upload upload = tm.upload(bucketName, inputKey, new File(inputFile));
        System.out.println("---****************--->"+s3Client.getUrl(bucketName, inputKey).toString());
        upload.waitForCompletion();
        System.out.println("---*--->"+s3Client.getUrl(bucketName, inputKey).toString());
        System.out.println("--**->"+upload.getProgress());
        System.out.println("---***--->"+s3Client.getUrl(bucketName, inputKey).toString());
        System.out.println("--****->"+upload.getState());
        System.out.println("---*****--->"+s3Client.getUrl(bucketName, inputKey).toString());
        System.out.println("--******->0");
        System.out.println("Object upload complete");
        System.out.println("--**********-->1");
    }

    private void uploadImageToS3() throws InterruptedException {
        System.out.println("Uploading media to S3 bucket ...");
        TransferManager tm = TransferManagerBuilder.standard().withS3Client(s3Client).build();
        Upload upload = tm.upload(bucketName, inputImageKey, new File(inputImageFile));
        System.out.println("---****************--->"+s3Client.getUrl(bucketName, inputImageKey).toString());
        upload.waitForCompletion();
        System.out.println("---*--->"+s3Client.getUrl(bucketName, inputImageKey).toString());
        System.out.println("--**->"+upload.getProgress());
        System.out.println("---***--->"+s3Client.getUrl(bucketName, inputImageKey).toString());
        System.out.println("--****->"+upload.getState());
        System.out.println("---*****--->"+s3Client.getUrl(bucketName, inputImageKey).toString());
        System.out.println("--******->0");
        System.out.println("Object upload complete");
        System.out.println("--**********-->1");
    }

    private void initAmazonS3Client() {
        System.out.println("Initializing amazon S3 client ...");
        // initialize credentials ///
        //ProfileCredentialsProvider provider = new ProfileCredentialsProvider();
        //awsCredentials = provider.getCredentials();
        // init amazon client
		AWSCredentials cr;
		if(aws_session_token == null) {
			cr = new BasicAWSCredentials(aws_access_key_id, aws_secret_access_key);
		} else {
			cr = new BasicSessionCredentials(aws_access_key_id, aws_secret_access_key, aws_session_token);
		}
        s3Client = AmazonS3ClientBuilder.standard()
                .withPathStyleAccessEnabled(true)
				.withCredentials(new AWSStaticCredentialsProvider(cr))
                .withRegion(region != null ? Regions.fromName(region) : Regions.US_EAST_1)
                .build();
    }

    private JobMediatorInput prepareExtractFacesMedatorJob() {
        // generate signed urls /////////////////
        System.out.println("Generating signed URLs ...");
        String inputSignedUrl = getS3PresignedUrl(bucketName, inputKey,  region);
			
		System.out.println("Generated input signed URL: " + inputSignedUrl);
		
        // fill job data /////////////
        System.out.println("Preparing extract faces job data ...");
        Locator video = new Locator()
                .awsS3Bucket(bucketName)
                .awsS3Key(inputKey)
                .httpEndpoint(inputSignedUrl);
														
        JobInput spJobInput = new ExtractFacesInput()
                .video(video)
				.effort(ExtractFacesInput.EffortEnum.LOW)
				.minimumFacesize((double)40)
				.faceSize((double)160);

        Job faceJob = new Job()
                .jobType(Job.JobTypeEnum.FACERECOGNITIONJOB)
                .jobProfile(Job.JobProfileEnum.EXTRACTFACES)
                .jobInput(spJobInput);

        JobMediatorInput jobMediatorInput = new JobMediatorInput()
                .projectServiceId(project_service_id)
                .quantity(file_duration)
				.tracking("tracking")
                .job(faceJob);

        return jobMediatorInput;
    }	
	
    private JobMediatorInput prepareClusterFacesMedatorJob(String faceExtractionId ) {
		
        // fill job data /////////////
        System.out.println("Preparing cluster faces job data for collectionId: " + faceExtractionId);
														
        JobInput spJobInput = new ClusterFacesInput()
                .faceExtractionId(faceExtractionId)
				.minimumClusterSize((double)2);

        Job faceJob = new Job()
                .jobType(Job.JobTypeEnum.FACERECOGNITIONJOB)
                .jobProfile(Job.JobProfileEnum.CLUSTERFACES)
                .jobInput(spJobInput);

        JobMediatorInput jobMediatorInput = new JobMediatorInput()
                .projectServiceId(project_service_id)
                .quantity(file_duration)
				.tracking("tracking")
                .job(faceJob);

        return jobMediatorInput;
    }		
	
    private JobMediatorInput prepareSearchFacesMedatorJob(String clusterCollectionId) {
        // generate signed urls /////////////////
        System.out.println("Generating signed URLs for search faces job ...");
        String inputSignedUrl = getS3PresignedUrl(bucketName, inputImageKey,  region);
			
		System.out.println("Generated input signed URL: " + inputSignedUrl);
		
        // fill job data /////////////
        System.out.println("Preparing search faces job data ...");
        Locator inputImage = new Locator()
                .awsS3Bucket(bucketName)
                .awsS3Key(inputImageKey)
                .httpEndpoint(inputSignedUrl);
														
        JobInput spJobInput = new SearchFacesInput()
                .inputImage(inputImage)
				.similarityThreshold((double)0.8F)
				.clusterCollectionId(clusterCollectionId);

        Job faceJob = new Job()
                .jobType(Job.JobTypeEnum.FACERECOGNITIONJOB)
                .jobProfile(Job.JobProfileEnum.SEARCHFACES)
                .jobInput(spJobInput);

        JobMediatorInput jobMediatorInput = new JobMediatorInput()
                .projectServiceId(project_service_id)
                .quantity(file_duration)
				.tracking("tracking")
                .job(faceJob);

        return jobMediatorInput;
    }		
	
    private void initializeDaletMediatorClient() throws ApiException {
        apiClient = Configuration.getDefaultApiClient();
        // update mediator current path
        if(basePath != null) {
            apiClient.setBasePath(basePath);
        }
		System.out.println("Using mediator address: " + apiClient.getBasePath());

        System.out.println("Receiving mediator access token ...");
        AuthApi apiInstance = new AuthApi(apiClient);
        Token token = apiInstance.getAccessToken(client_id, client_secret);

        ApiKeyAuth tokenSignature = (ApiKeyAuth) apiClient.getAuthentication("tokenSignature");
        tokenSignature.setApiKey(token.getAuthorization());

        jobsApi = new JobsApi(apiClient);
        faceRecognitionApi = new FaceRecognitionApi(apiClient);
    }

    private  String getS3PresignedUrl(String bucket, String key,  String region) {
        java.util.Date expiration = new java.util.Date();
        long expTimeMillis = expiration.getTime();
        expTimeMillis += 1000 * 60 * 60;// Set the presigned URL to expire after one hour.
        expiration.setTime(expTimeMillis);

/*
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withPathStyleAccessEnabled(true)
                .withRegion(region != null ? Regions.fromName(region) : Regions.US_EAST_1)
                .build();
*/
		System.out.println("Generate signed URL for key: " + key);
        URL url = s3Client.generatePresignedUrl(new GeneratePresignedUrlRequest(bucket, key)
                .withMethod(HttpMethod.GET).withExpiration(expiration));
        return url.toString();
    }
}
