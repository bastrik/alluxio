/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.cli.fs.FileSystemShellUtils;
import alluxio.cli.fs.command.job.JobAttempt;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.job.JobMasterClient;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.ListStatusPOptions;
import alluxio.job.JobConfig;
import alluxio.job.plan.load.LoadConfig;
import alluxio.job.wire.JobInfo;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Loads a file or directory in Alluxio space, makes it resident in memory.
 */
@ThreadSafe
@PublicApi
public final class DistributedLoadCommand extends AbstractDistributedJobCommand {
  private static final int DEFAULT_REPLICATION = 1;
  private static final Option REPLICATION_OPTION =
      Option.builder()
          .longOpt("replication")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .type(Number.class)
          .argName("replicas")
          .desc("Number of block replicas of each loaded file, default: " + DEFAULT_REPLICATION)
          .build();

  private static final Option INDEXFILE_OPTION =
      Option.builder()
          .longOpt("indexfile")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .type(Number.class)
          .argName("indexfile")
          .desc("Index file containing files to be loaded. The file path in the index file "
            + "is relative path to the folder path for distributedload. "
            + "By default all files will be loaded.")
          .build();

  /**
   * Constructs a new instance to load a file or directory in Alluxio space.
   *
   * @param fsContext the filesystem of Alluxio
   */
  public DistributedLoadCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "distributedLoad";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(REPLICATION_OPTION).addOption(INDEXFILE_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    int replication = FileSystemShellUtils.getIntArg(cl, REPLICATION_OPTION, DEFAULT_REPLICATION);

    String indexfile = cl.getOptionValue(INDEXFILE_OPTION.getLongOpt());
    if (indexfile != null) {
      System.out.println("DistributedLoad based on index file: " + indexfile);
    }
    distributedLoad(path, replication, indexfile);
    return 0;
  }

  @Override
  public void close() throws IOException {
    mClient.close();
  }

  /**
   * Creates a new job to load a file in Alluxio space, makes it resident in memory.
   *
   * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
   * @param replication The replication of file to load into Alluxio memory
   */
  private LoadJobAttempt newJob(AlluxioURI filePath, int replication) {
    LoadJobAttempt jobAttempt = new LoadJobAttempt(mClient, new
        LoadConfig(filePath.getPath(), replication), new CountingRetry(3));

    jobAttempt.run();

    return jobAttempt;
  }

  /**
   * Add one job.
   */
  private void addJob(URIStatus status, int replication) {
    AlluxioURI filePath = new AlluxioURI(status.getPath());
    if (status.getInAlluxioPercentage() == 100) {
      // The file has already been fully loaded into Alluxio.
      System.out.println(filePath + " is already fully loaded in Alluxio");
      return;
    }
    if (mSubmittedJobAttempts.size() >= mActiveJobs) {
      // Wait one job to complete.
      waitJob();
    }
    System.out.println(filePath + " loading");
    mSubmittedJobAttempts.add(newJob(filePath, replication));
  }

  /**
   * Distributed loads a file or directory in Alluxio space, makes it resident in memory.
   *
   * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
   * @param replication The replication of file to load into Alluxio memory
   * @param indexFile file The index file containing all files to load
   */
  private void distributedLoad(AlluxioURI filePath, int replication, String indexFile)
      throws AlluxioException, IOException {
    load(filePath, replication, indexFile);
    // Wait remaining jobs to complete.
    drain();
  }

  /**
   * Loads a file or directory in Alluxio space, makes it resident in memory.
   *
   * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
   * @param replication The replication of file to load into Alluxio memory
   * @param indexFile file The index file containing all files to load
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException      when non-Alluxio exception occurs
   */
  private void load(AlluxioURI filePath, int replication, String indexFile)
      throws IOException, AlluxioException {
    ListStatusPOptions options = ListStatusPOptions.newBuilder().setRecursive(true).build();
    HashSet<String> filesToLoad = loadIndexFile(indexFile);

    mFileSystem.iterateStatus(filePath, options, uriStatus -> {
      if (!uriStatus.isFolder() && (filesToLoad == null
          || filesToLoad.contains(getRelativePath(filePath.getPath(), uriStatus.getPath())))) {
        addJob(uriStatus, replication);
      }
    });
  }

  private String getRelativePath(String rootPath, String fullPath) {
    int index = rootPath.length();
    if (!rootPath.endsWith("/")) {
      index += 1;
    }

    return fullPath.substring(index);
  }

  /**
   * Load file paths in index file into Hashset.
   *
   * @param indexFile index file containing file paths. One file path per line
   * @return Hashset with file paths listed in the index file. Returns null if indexFile is null
   */
  private HashSet<String> loadIndexFile(String indexFile) throws IOException {
    if (indexFile == null || indexFile.trim().isEmpty()) {
      return null;
    }

    RandomAccessFile file = new RandomAccessFile(indexFile, "r");
    HashSet<String> pathes = new HashSet<String>();
    String str;
    while ((str = file.readLine()) != null) {
      if (!str.trim().isEmpty()) {
        pathes.add(str);
      }
    }
    return pathes;
  }

  @Override
  public String getUsage() {
    return "distributedLoad [--replication <num>]i [--indexfile <local file path>] <path>";
  }

  @Override
  public String getDescription() {
    return "Loads a file or all files in a directory into Alluxio space.";
  }

  private class LoadJobAttempt extends JobAttempt {
    private LoadConfig mJobConfig;

    LoadJobAttempt(JobMasterClient client, LoadConfig jobConfig, RetryPolicy retryPolicy) {
      super(client, retryPolicy);
      mJobConfig = jobConfig;
    }

    @Override
    protected JobConfig getJobConfig() {
      return mJobConfig;
    }

    @Override
    protected void logFailedAttempt(JobInfo jobInfo) {
      System.out.println(String.format("Attempt %d to load %s failed because: %s",
          mRetryPolicy.getAttemptCount(), mJobConfig.getFilePath(),
          jobInfo.getErrorMessage()));
    }

    @Override
    protected void logFailed() {
      System.out.println(String.format("Failed to complete loading %s after %d retries.",
          mJobConfig.getFilePath(), mRetryPolicy.getAttemptCount()));
    }

    @Override
    protected void logCompleted() {
      System.out.println(String.format("Successfully loaded path %s after %d attempts",
          mJobConfig.getFilePath(), mRetryPolicy.getAttemptCount()));
    }
  }
}
