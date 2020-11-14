package io.github.manuzhang

case class JobStatus(status: String, reason: String = "")


object JobStatus {
  val SUCCEEDED = JobStatus("SUCCEEDED")
  val UNKNOWN = JobStatus("UNKNOWN")
  val RUNNING = JobStatus("RUNNING")
  def fail(ex: Throwable): JobStatus = {
    JobStatus("FAILED", ex.toString())
  }
}
