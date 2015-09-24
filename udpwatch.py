#!/usr/bin/env python

import os, sys, fcntl, socket, subprocess, struct, signal, time, logging
import datetime
from glob import glob
from ConfigParser import ConfigParser

#######################################################################
# CONFIG
#######################################################################

# Where to make a log file
LOGDIR  = "/appl/logs/transcoder/"
LOGFILE = LOGDIR + str(datetime.date.today()) + "_udpwatch.log"

########################################################################
########################################################################
########################################################################

def script_running(lockfile):
  global file_handle
  file_handle = open(lockfile, 'w')
  try:
    fcntl.lockf(file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    return False
  except IOError:
    return True

def setup_logging():
  logging.NORMAL = 25
  logging.addLevelName(logging.NORMAL, "NORMAL")

  logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s - %(message)s', level=logging.INFO, filename = LOGFILE)
  #logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s - %(message)s', level=logging.INFO)

  logger = logging.getLogger("Transcoder")
  logger.normal = lambda msg, *args: logger._log(logging.NORMAL, msg, args)
  return logger

def create_udp_socket(ip, port, timeout):
  try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', port))
    mreq = struct.pack("=4sl", socket.inet_aton(ip), socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    sock.settimeout(timeout)
    return sock
  except socket.error as msg:
    logger.error(msg)
    sock.close()
    sock = None

def get_enabled_channels(confdir):
  CHANNELS = {}

  for config_file in glob(confdir + "*.ini"):
    config = ConfigParser()
    config.read(config_file)

    PATH, NAME   = os.path.split(config_file)
    NAME, EXT    = os.path.splitext(NAME)
    PID          = get_ffmpeg_pid(config.get('General', 'MCAST_OUT_IP'), config.get('General', 'MCAST_OUT_PORT'))

    CHANNELS[NAME] = {
          "NAME":          NAME,
          "PID":           PID,
          "MCAST_IP":      config.get('General', 'MCAST_OUT_IP'), 
          "MCAST_PORT":    config.get('General', 'MCAST_OUT_PORT'),
          "INPUT_STREAM":  config.get('General', 'INPUT_STREAM'),
          "VIDEO_BITRATE": config.get('General', 'VIDEO_BITRATE'),
          "AUDIO_BITRATE": config.get('General', 'AUDIO_BITRATE'),
          "CODEC":         config.get('General', 'CODEC'),
          "VIDEO_MAPPING": config.get('General', 'VIDEO_MAPPING'),
          "AUDIO_MAPPING": config.get('General', 'AUDIO_MAPPING'),
          "MUXRATE":       config.get('General', 'MUXRATE'),
          "LOGLEVEL":      config.get('General', 'LOGLEVEL')
          }
  return CHANNELS

def get_ffmpeg_pid(ip, port):
  p = subprocess.Popen(['pgrep', '-f' , ip+":"+str(port)], stdout=subprocess.PIPE)
  pid, err = p.communicate()
  if pid:
    #return int(pid.rstrip())
    return pid.rstrip()
  else:
    return False

def kill_pid(pid, channel_name):
  logger.warning("%s Killing PID %s", channel_name, pid)
  os.kill(int(pid), signal.SIGKILL)

def check_udp_output(channel_name, mcast_ip, mcast_port, udp_data_timeout, probe_time):
  logger.debug("Check output started")

  PID = get_ffmpeg_pid(mcast_ip, mcast_port)

  if PID != False:
    logger.debug("%s PID %s is already running with %s:%s", channel_name, PID, mcast_ip, mcast_port)

    # Create a UDP listening socket
    s = create_udp_socket(mcast_ip, mcast_port, udp_data_timeout)

    startTime = time.time()

    while time.time() - startTime < probe_time:
      try:
        data = False
        data = s.recv(10240)
        logger.debug("%s PID %s Received %s bytes on %s:%s", channel_name, PID, len(data), mcast_ip, mcast_port)
        #continue

      except KeyboardInterrupt:
        logger.info("Closing UDP socket")
        s.close()
        logger.info("Script terminated")
        sys.exit(0)

      except socket.timeout:
        # socket receive timed out, means there's no data coming on that UDP
        logger.error("%s PID %s - No mcast output on %s:%s", channel_name, PID, mcast_ip, mcast_port)

        # Need to get the PID again here, to make sure there's something to kill,
        # because ffmpeg might have died completely
        PID = get_ffmpeg_pid(mcast_ip, mcast_port)

        if PID != False:
          kill_pid(PID, channel_name)

        # and break out from the while loop
        break

      except socket.error:
        # some other error happened on the socket
        logger.error("%s Socket error", channel_name)
        break
    # END of while

    if data != False:
      # if there's UDP data again, let's log NORMAL message
      logger.normal("%s PID %s is running with %s:%s", channel_name, PID, mcast_ip, mcast_port)

  else:
    logger.error("%s %s:%s is not running.", channel_name, mcast_ip, mcast_port)

def main():
  # some dirty commandline argument parser ;)
  if len(sys.argv) < 2:
    logger.error("No arguments - Please specify command line arguments")
    logger.info(sys.argv[0] + " <CHANNEL_NAME> <MCAST_IP> <MCAST_PORT> <UDP_DATA_TIMEOUT> <PROBE_TIME>")
    logger.info("Example: " + sys.argv[0] + " 239.255.14.5 3199 RCKTV 5 10")
    logger.info("Exiting...")
    sys.exit(1)
  else:
    if script_running("/dev/shm/" + str(sys.argv[1]) + "_udpwatch.lock"):
      logger.warning("Script is already running - exiting...")
      sys.exit(0)

    logger.debug("We have arguments: %s", sys.argv)
    check_udp_output(str(sys.argv[1]), str(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5]))

####################################################################
# MAIN #############################################################
####################################################################

# setup logging
logger = setup_logging()

# prevent multiple instances
file_handle = None

if __name__ == '__main__':
  main()
