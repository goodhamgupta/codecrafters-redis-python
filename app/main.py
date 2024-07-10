import socket
from threading import Thread
from typing import Tuple, Optional, List
import time
import argparse
from argparse import Namespace
from pathlib import Path
import struct


MAX_BYTES_TO_RECEIVE = 1024
RDB_FILE_SIZE_BYTES = 93
MAX_NUM_UNACCEPTED_CONN = 10
ARGS_IDX = 0
CMD_LEN_IDX = 0
CMD_IDX = 1
PARAM_LEN_IDX = 2
PARAM_IDX = 3
PARAM_ARG_LEN_IDX = 4
PARAM_ARG_IDX = 5
EXTRA_ARGS_CMD_LEN_IDX = 6
EXTRA_ARGS_CMD_IDX = 7
EXTRA_ARGS_CONTENT_LEN_IDX = 8
EXTRA_ARGS_CONTENT_IDX = 9
SECONDS_TO_MS = 1_000
NUM_BYTES_RECEIVED_SO_FAR = 0


class Parser:
    MASTER_PORT = 6379
    REDIS_DB = {}
    STREAM_DB = {}

    REPLICATION_ID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
    REPLICATION_OFFSET = 0

    RDB_HEX_DUMP = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

    REPLICA_SOCKETS = []

    def __init__(
        self,
        client_socket: socket.socket,
        cmd_list: List[str],
        cmd_bytes: bytes,
        args: Namespace,
        restored_kv_pairs: List[Tuple],
    ):
        """
        Initializes the Parser instance.

        Args:
            cmd_list (List[str]): The list of command arguments received from the client.
            args (Namespace): Additional arguments for the parser.
        """
        self.client_socket = client_socket.dup()
        self.cmd_list = cmd_list
        self.cmd_bytes = cmd_bytes
        self.client_port = client_socket.getsockname()[1]
        self.args = args
        self.role = "REPLICA" if self.args.replicaof else "MASTER"

        if restored_kv_pairs:
            current_time = time.time() * SECONDS_TO_MS
            print(
                f"[{self.role}] Restoring key-value pairs from RDB dump. Current time: {current_time}"
            )

            for ckey, cvalue, cttl in restored_kv_pairs:
                print(f"[{self.role}] Restoring key: {ckey} with value: {cvalue}")

                if len(ckey) > 0:
                    if cttl:
                        if current_time > cttl:
                            print(
                                f"[{self.role}] TTL expired for key: {ckey} and value: {cvalue}. TTL: {cttl} Current time: {current_time}"
                            )
                            continue
                        else:
                            print(
                                f"[{self.role}] Restoring key: {ckey} with value: {cvalue} and TTL: {cttl}"
                            )
                            self.REDIS_DB.setdefault(ckey, {}).update(
                                {"value": cvalue, "TTL": cttl}
                            )
                    else:
                        self.REDIS_DB.setdefault(ckey, {}).update({"value": cvalue})

    def _extract_content(
        self, cmd_list: List[str], content_len_idx: int, content_idx: int
    ) -> Tuple[Optional[int], Optional[str]]:
        """
        Extracts the content from the command list based on the provided indices.

        Args:
            cmd_list (List[str]): The list of command arguments.
            content_len_idx (int): The index in the command list where the length of the content is specified.
            content_idx (int): The index in the command list where the actual content is located.

        Returns:
            Tuple[Optional[int], Optional[str]]: A tuple containing the length of the content and the content itself.
            If the indices are incorrect, returns (None, None).

        Raises:
            AssertionError: If the actual length of the content does not match the specified length.
        """
        if len(cmd_list) > content_idx:
            param_len = int(cmd_list[content_len_idx][1:])
            param_content = cmd_list[content_idx]
            assert (
                len(param_content) == param_len
            ), f"Expected parameter of length {param_len} but received {len(param_content)}"
            return param_len, param_content
        else:
            print(
                f"[{self.role}] Arguments idx incorrect. Command list has {len(cmd_list)} elements but requested idx is {content_idx}"
            )
            return (None, None)

    def parse_command(self) -> Optional[bytes]:
        """
        Parses the command list and dispatches the command to the appropriate handler.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response from the command handler.

        Raises:
            Exception: If the command is not supported.
        """
        cmd_lists = []
        i = 0
        while i < len(self.cmd_list):
            if self.cmd_list[i].startswith("*"):
                num_elements = int(self.cmd_list[i][1:])
                cmd_list = []
                j = i + 1
                end_idx = j + num_elements * 2
                while j < end_idx and j < len(self.cmd_list):
                    cmd_list.append(self.cmd_list[j])
                    j += 1
                cmd_lists.append(cmd_list)
                i = j  # Move index past the current command block
            else:
                i += 1

        if len(cmd_lists) == 0:
            print(f"[{self.role}] FALLBACK! Assigning current command to cmd_lists")
            cmd_lists = [self.cmd_list.copy()]
        print(f"[{self.role}] Processed CMD LISTS: ", cmd_lists)
        for cmd_list in cmd_lists:
            print(f"[{self.role}] Received command list: ", cmd_list)
            cmd = cmd_list[CMD_IDX].strip().upper()
            cmd_len = int(cmd_list[CMD_LEN_IDX][1:])
            assert (
                len(cmd) == cmd_len
            ), f"Expected argument of length {cmd_len} but received {len(cmd)}"

            command_functions = {
                "PING": self._handle_ping,
                "ECHO": self._handle_echo,
                "SET": self._handle_set,
                "GET": self._handle_get,
                "INFO": self._handle_info,
                "REPLCONF": self._handle_replconf,
                "PSYNC": self._handle_psync,
                "WAIT": self._handle_wait,
                "CONFIG": self._handle_config,
                "KEYS": self._handle_keys,
                "TYPE": self._handle_type,
                "XADD": self._handle_xadd,
            }
            last_result = None

            if cmd in command_functions:
                result = command_functions[cmd](cmd_list)
                print(f"[{self.role}] {cmd} response in parse_command: ", result)
                if isinstance(result, bytes):
                    print(f"[{self.role}] Sending response for {cmd}..")
                    self.client_socket.send(result)
                    print(f"[{self.role}] Sent response for {cmd}: {result}")
                    last_result = result
                else:
                    print(f"[{self.role}] Received null result for {cmd}")

                if self.role == "REPLICA":
                    global NUM_BYTES_RECEIVED_SO_FAR
                    cmd_count = sum(list(map(lambda x: 1 if "$" in x else 0, cmd_list)))
                    cur_cmd_bytes = (
                        f"*{cmd_count}\r\n" + "\r\n".join(cmd_list) + "\r\n"
                    ).encode("utf-8")
                    print("cur_cmd_bytes: ", cur_cmd_bytes)
                    print("length of cur_cmd_bytes: ", len(cur_cmd_bytes))
                    print(
                        f"Current num_bytes_received_so_far: {NUM_BYTES_RECEIVED_SO_FAR}.  Updating.."
                    )
                    NUM_BYTES_RECEIVED_SO_FAR += len(cur_cmd_bytes)
                    print(
                        "Updated num_bytes_received_so_far: ", NUM_BYTES_RECEIVED_SO_FAR
                    )
            else:
                raise Exception(f"Command {cmd} not supported!")
            # Track the number of bytes received so far
            print(f"[{self.role}] {cmd} processed successfully!")
            return last_result

    def _handle_ping(self, _cmd_list) -> Optional[bytes]:
        """
        Handles the PING command.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response "+PONG\r\n".
        """
        if self.role == "REPLICA":
            print("[REPLICA] Received PING from MASTER. Will not respond.")
            return
        return b"+PONG\r\n"

    def _handle_echo(self, cmd_list) -> bytes:
        """
        Handles the ECHO command.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response containing the echoed message.
        """
        param_len, param_content = self._extract_content(
            cmd_list, PARAM_LEN_IDX, PARAM_IDX
        )
        return f"${param_len}\r\n{param_content}\r\n".encode("utf-8")

    def _handle_set(self, cmd_list) -> Optional[bytes]:
        """
        Handles the SET command.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response "+OK\r\n".
        """
        key_len, key_content = self._extract_content(cmd_list, PARAM_LEN_IDX, PARAM_IDX)
        val_len, val_content = self._extract_content(
            cmd_list, PARAM_ARG_LEN_IDX, PARAM_ARG_IDX
        )
        extra_args_len, extra_args_content = self._extract_content(
            cmd_list, EXTRA_ARGS_CMD_LEN_IDX, EXTRA_ARGS_CMD_IDX
        )
        record = {}
        if extra_args_content:
            if extra_args_content.strip().upper() == "PX":
                # Add TTL
                _ttl_len, ttl_content = self._extract_content(
                    cmd_list, EXTRA_ARGS_CONTENT_LEN_IDX, EXTRA_ARGS_CONTENT_IDX
                )
                if ttl_content:
                    record.update(
                        {
                            "value": val_content,
                            "TTL": (time.time() * SECONDS_TO_MS) + float(ttl_content),
                        }
                    )
                else:
                    raise Exception("TTL not provided for SET PX command")
        else:
            record.update({"value": val_content})
        self.REDIS_DB.update({key_content: record})
        print("*****************************")
        print(f"[{self.role}] Updated DB: ", self.REDIS_DB)
        print("*****************************")
        if len(Parser.REPLICA_SOCKETS) > 0:
            for replica_socket, replica_port in Parser.REPLICA_SOCKETS:
                print(
                    f"[{self.role}] Sending command to replica: {replica_socket} on port {replica_port}..."
                )
                cmd_count = sum(list(map(lambda x: 1 if "$" in x else 0, cmd_list)))
                print("Original cmd_list: ", cmd_list)
                replica_command = "\r\n".join([f"*{cmd_count}"] + cmd_list) + "\r\n"
                print(
                    f"[{self.role}] Replica SET command bytes: {replica_command}".encode(
                        "utf-8"
                    )
                )
                replica_socket.sendall(replica_command.encode("utf-8"))
                print(f"Message sent to replica for command: {cmd_list}")

        print(f"[{self.role}] SET command processed successfully!")
        if self.role == "REPLICA":
            print(f"[{self.role}] Command received on replica. WON'T SEND A RESPONSE")
            return None
            # return b"$-1\r\n"
        else:
            print("Returning OK")
        return b"+OK\r\n"

    def _handle_get(self, cmd_list) -> bytes:
        """
        Handles the GET command.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response containing the value associated with the key, or "$-1\r\n" if the key is not found.
        """
        _key_len, key_content = self._extract_content(
            cmd_list, PARAM_LEN_IDX, PARAM_IDX
        )
        current_time = time.time() * SECONDS_TO_MS
        print(
            f"[{self.role}] IN GET COMMAND, cur client: {self.client_socket} current DB: {self.REDIS_DB} Current time: {current_time}"
        )
        value_struct = self.REDIS_DB.get(key_content, None)
        if value_struct is None:
            print(f"[{self.role}] Returning null because value_struct is None")
            return b"$-1\r\n"
        elif (
            "TTL" in value_struct and value_struct["TTL"] < time.time() * SECONDS_TO_MS
        ):
            print(f"[{self.role}] Returning null because value TTL expired")
            self.REDIS_DB.pop(key_content, None)
            return b"$-1\r\n"
        else:
            return (
                f"${len(value_struct['value'])}\r\n{value_struct['value']}\r\n".encode(
                    "utf-8"
                )
            )

    def _handle_info(self, cmd_list) -> bytes:
        """
        Handles the INFO command.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response containing the role of the server.
        """
        if self.role == "REPLICA":
            return b"$10\r\nrole:slave\r\n"
        else:
            return f"$87\r\nrole:master\nmaster_replid:{self.REPLICATION_ID}\nmaster_repl_offset:{self.REPLICATION_OFFSET}\r\n".encode(
                "utf-8"
            )

    def _handle_replconf(self, cmd_list) -> Optional[bytes]:
        """
        Handles the REPLCONF command.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response indicating the REPLCONF command was handled successfully.
        """
        _cmd_len, cmd = self._extract_content(cmd_list, PARAM_LEN_IDX, PARAM_IDX)
        if cmd:
            cmd = cmd.strip().upper()
            if cmd == "LISTENING-PORT":
                print(
                    f"[{self.role}] Received REPLCONF listening-port cmd. Registering replica.."
                )
                _port_len, port_str = self._extract_content(
                    cmd_list, PARAM_ARG_LEN_IDX, PARAM_ARG_IDX
                )
                if port_str:
                    Parser.REPLICA_SOCKETS.append(
                        (self.client_socket.dup(), int(port_str))
                    )
                    print(
                        f"[{self.role}] Replica register. Socket: ", self.client_socket
                    )
                else:
                    raise Exception(
                        "Port not provided for REPLCONF listening-port command"
                    )
                return b"+OK\r\n"
            elif cmd == "CAPA":
                print(f"[{self.role}] Received REPLCONF capa cmd. Sending ACK..")
                return b"+OK\r\n"
            elif cmd == "GETACK":
                if self.role == "REPLICA":
                    print(f"[{self.role}] Received REPLCONF GETACK cmd. Sending ACK..")
                    num_bytes_len = len(str(NUM_BYTES_RECEIVED_SO_FAR))
                    print(
                        f"[{self.role}] Bytes received so far: {NUM_BYTES_RECEIVED_SO_FAR}"
                    )
                    return f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${num_bytes_len}\r\n{NUM_BYTES_RECEIVED_SO_FAR}\r\n".encode(
                        "utf-8"
                    )
                else:
                    print(
                        f"[{self.role}] Received REPLCONF GETACK cmd on MASTER. Ignoring.."
                    )
                    return None
        else:
            raise Exception(f"Unknown REPLCONF command: {cmd}")

    def _handle_psync(self, _cmd_list) -> None:
        """
        Handles the PSYNC command.

        Args:
            _cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response indicating the PSYNC command was handled successfully.
        """
        rdb_binary = bytes.fromhex(self.RDB_HEX_DUMP)
        fullresync_response = [
            f"+FULLRESYNC {self.REPLICATION_ID} {self.REPLICATION_OFFSET}\r\n".encode(
                "utf-8"
            ),
            f"${len(rdb_binary)}\r\n".encode("utf-8") + rdb_binary,
        ]
        for response in fullresync_response:
            print(f"[{self.role}] Sending response: {response}")
            self.client_socket.sendall(response)

    def _send_replconf_getack(self, replica_socket: socket.socket) -> bool:
        """
        Send a REPLCONF GETACK command to a replica and wait for its response.

        This method sends a REPLCONF GETACK command to the specified replica socket,
        waits for the response, and checks if it's a valid REPLCONF ACK.

        Args:
            replica_socket (socket.socket): The socket connected to the replica.

        Returns:
            bool: True if a valid ACK is received, False otherwise.

        Raises:
            Exception: If there's an error in communication with the replica.
        """
        try:
            print(f"[{self.role}] Sending REPLCONF GETACK to replica..")
            replica_socket.sendall(
                b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
            )
            print(f"[{self.role}] Sent REPLCONF GETACK to replica")

            print(f"[{self.role}] Waiting for response from replica..")
            response = replica_socket.recv(MAX_BYTES_TO_RECEIVE).decode("utf-8")
            print(f"[{self.role}] Received response from replica: {response}")

            if response.startswith("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n"):
                return True
            else:
                print(f"[{self.role}] Unexpected response from replica: {response}")
                return False
        except Exception as e:
            print(f"[{self.role}] Error communicating with replica: {e}")
            return False

    def _handle_wait(self, cmd_list) -> bytes:
        """
        Handles the WAIT command.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response containing the number of replicas that acknowledged the command.
        """
        num_replicas = int(cmd_list[PARAM_IDX])
        timeout = (
            float(cmd_list[PARAM_ARG_IDX]) / SECONDS_TO_MS
        )  # Convert milliseconds to seconds
        print(
            f"[{self.role}] WAIT command received. Waiting for {num_replicas} replicas with timeout {timeout} seconds"
        )

        start_time = time.time()
        acks = 0

        for replica_socket, _ in self.REPLICA_SOCKETS:
            print(f"[{self.role}] Sending GETACK to replica: {replica_socket}")
            if self._send_replconf_getack(replica_socket):
                acks += 1
                print(f"[{self.role}] Received ACK from replica. Total ACKs: {acks}")
                if acks >= num_replicas:
                    print(f"[{self.role}] Received enough ACKs. Breaking loop.")
                    break
            else:
                print(
                    f"[{self.role}] Failed to receive ACK from replica: {replica_socket}"
                )

            if time.time() - start_time > timeout:
                print(f"[{self.role}] Timeout reached. Breaking loop.")
                break

        response = f":{acks}\r\n".encode("utf-8")
        print(f"[{self.role}] WAIT command completed. Returning: {response}")
        return response

    def _handle_config(self, cmd_list) -> bytes:
        """
        Handles the CONFIG command.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response containing the configuration value.
        """
        config_key = cmd_list[PARAM_IDX]
        value = cmd_list[PARAM_ARG_IDX]
        print(
            f"[{self.role}] CONFIG command received. Key: {config_key} and value: {value}"
        )
        if config_key == "GET" and value == "dir":
            return f"*2\r\n$3\r\ndir\r\n${len(self.args.dir)}\r\n{self.args.dir}\r\n".encode(
                "utf-8"
            )
        elif config_key == "GET" and value == "dbfilename":
            return f"*2\r\n$10\r\ndbfilename\r\n${len(self.args.dbfilename)}\r\n{self.args.dbfilename}\r\n".encode(
                "utf-8"
            )
        else:
            raise Exception(f"Unknown CONFIG command: {config_key}")

    def _handle_keys(self, _cmd_list) -> bytes:
        """
        Handles the KEYS command.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response containing the keys.
        """
        keys = list(self.REDIS_DB.keys())
        if len(keys) == 0:
            return b"*0\r\n"
        resp_array = f"*{len(keys)}\r\n" + "".join(
            [f"${len(key)}\r\n{key}\r\n" for key in keys]
        )
        return resp_array.encode("utf-8")

    def _handle_type(self, cmd_list) -> bytes:
        """
        Handles the TYPE command.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response containing the type of the key.
                   Possible return values:
                   - b"+string\r\n": If the key exists and its value is a string.
                   - b"+none\r\n": If the key does not exist in the database.
                   - Raises an Exception: If the value type is unsupported.
        """
        key = cmd_list[PARAM_IDX]
        print(f"[{self.role}] TYPE command received. Key: {key}")
        if key in self.REDIS_DB:
            value = self.REDIS_DB[key]["value"]
            if isinstance(value, str):
                return b"+string\r\n"
            else:
                raise Exception(f"Unsupported value type: {type(value)}")
        elif key in self.STREAM_DB:
            return b"+stream\r\n"
        else:
            return b"+none\r\n"

    def _generate_stream_id_ms_time(self, candidate_ms_time, stream_value):
        """
        Generate the millisecond time part of a stream ID.

        Args:
            candidate_ms_time (str): The candidate millisecond time, can be "*" for auto-generation.
            stream_value (list): The current stream entries.

        Returns:
            int: The generated millisecond time.

        This method handles the generation of the millisecond time part of a stream ID.
        It takes into account the existing entries in the stream and the provided candidate time.

        If candidate_ms_time is "*", it auto-generates a time based on the following rules:
        1. If there are existing entries, it returns the time of the last entry.
        2. If there are no existing entries, it returns the current time in milliseconds.

        If candidate_ms_time is not "*", it returns the integer value of candidate_ms_time.
        """
        if len(stream_value) > 0:
            last_stream = stream_value[-1]
            ms_time, _ = map(int, last_stream["stream_id"].split("-"))
        else:
            ms_time = None

        if candidate_ms_time == "*":
            if ms_time is not None:
                return ms_time
            else:
                return int(time.time() * SECONDS_TO_MS)

        try:
            return int(candidate_ms_time)
        except ValueError:
            raise ValueError(
                f"Invalid candidate_ms_time: {candidate_ms_time}. Must be '*' or a valid integer."
            )

    def _generate_stream_id_seq_num(
        self, generated_ms_time, candidate_seq_num, stream_value
    ):
        """
        Generate the sequence number part of a stream ID.

        Args:
            generated_ms_time (int): The millisecond time part of the stream ID.
            candidate_seq_num (str): The candidate sequence number, can be "*" for auto-generation.
            stream_value (list): The current stream entries.

        Returns:
            int: The generated sequence number.

        This method handles the generation of the sequence number part of a stream ID.
        It takes into account the existing entries in the stream and the provided candidate sequence number.

        If candidate_seq_num is "*", it auto-generates a sequence number based on the following rules:
        1. If there are existing entries and the generated_ms_time matches the last entry's time,
           it increments the last sequence number.
        2. If generated_ms_time is 0, it returns 1.
        3. Otherwise, it returns 0.

        If candidate_seq_num is not "*", it returns the integer value of candidate_seq_num.
        """
        ms_time, seq_num = None, None
        if len(stream_value) > 0:
            last_stream = stream_value[-1]
            [ms_time, seq_num] = [int(x) for x in last_stream["stream_id"].split("-")]
        if candidate_seq_num == "*":
            print(f"[{self.role}] Auto-generating sequence number for XADD command")
            if seq_num is not None:
                if generated_ms_time == ms_time:
                    return seq_num + 1
                elif generated_ms_time == 0:
                    return 1
                else:
                    return 0
            else:
                print(
                    f"[{self.role}] No existing sequence number. Received generated_ms_time: {generated_ms_time} "
                )
                if generated_ms_time == 0:
                    print(f"[{self.role}] Returning 1 as sequence number")
                    return 1
                print(f"[{self.role}] Returning 0 as sequence number")
                return 0
        else:
            return int(candidate_seq_num)

    def _handle_xadd(self, cmd_list) -> bytes:
        """
        Handles the XADD command for adding new entries to a stream.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response containing the ID of the added entry.

        Raises:
            Exception: If the stream ID is not provided.

        This method processes the XADD command, which adds a new entry to a Redis stream.
        It performs the following steps:
        1. Extracts the stream key and ID from the command list.
        2. Validates the stream ID.
        3. Generates a new stream ID if necessary.
        4. Adds the new entry to the stream.
        5. Returns the ID of the newly added entry.

        The method also handles error cases, such as invalid stream IDs or
        attempts to add entries with IDs smaller than or equal to existing entries.
        """
        (_stream_key_len, stream_key) = self._extract_content(
            cmd_list, PARAM_LEN_IDX, PARAM_IDX
        )
        (_stream_id_len, stream_id) = self._extract_content(
            cmd_list, PARAM_ARG_LEN_IDX, PARAM_ARG_IDX
        )
        print(f"[{self.role}] Current stream DB: {self.STREAM_DB}")
        if stream_id:
            print(
                f"[{self.role}] XADD command received. Key: {stream_key} and ID: {stream_id}"
            )
            if stream_id == "*":
                [candidate_ms_time, candidate_seq_num] = ["*", "*"]
            else:
                [candidate_ms_time, candidate_seq_num] = [
                    x for x in stream_id.split("-")
                ]
            if candidate_ms_time == "0" and candidate_seq_num == "0":
                return b"-ERR The ID specified in XADD must be greater than 0-0\r\n"
            stream_value = self.STREAM_DB.get(stream_key, [])
            generated_ms_time = self._generate_stream_id_ms_time(
                candidate_ms_time, stream_value
            )
            generated_seq_num = self._generate_stream_id_seq_num(
                generated_ms_time, candidate_seq_num, stream_value
            )
            generated_stream_id = f"{generated_ms_time}-{generated_seq_num}"
            print(f"[{self.role}] Generated stream ID: {generated_stream_id}")
            if len(stream_value) > 0:
                # Validate if current stream ID is greater than the last stream ID
                last_stream = stream_value[-1]
                [ms_time, seq_num] = [
                    int(x) for x in last_stream["stream_id"].split("-")
                ]
                if (generated_ms_time > ms_time) or (
                    generated_ms_time == ms_time and generated_seq_num > seq_num
                ):
                    print(f"[{self.role}] Stream ID is valid. Adding to stream.")
                    stream_value.append({"stream_id": generated_stream_id})
                else:
                    return b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
            else:
                stream_value.append({"stream_id": generated_stream_id})

            self.STREAM_DB[stream_key] = stream_value
            return f"${len(generated_stream_id)}\r\n{generated_stream_id}\r\n".encode(
                "utf-8"
            )
        else:
            raise Exception("Stream ID not provided for XADD command")


def process_request(client_socket, _client_addr, args, restored_kv_pairs):
    """
    Processes the request from a client. It continuously receives data from the client
    and sends a response until the client socket is closed.

    Args:
        client_socket (socket): The client's socket.
        client_addr (tuple): The client's address.

    Returns:
        None
    """
    role = "REPLICA" if args.replicaof else "MASTER"
    try:
        while True:
            recv_bytes = client_socket.recv(MAX_BYTES_TO_RECEIVE)
            if not recv_bytes:
                break
            data = recv_bytes.decode("utf-8")
            cmd_list = data.split("\r\n")
            print("In process request, received command: ", cmd_list)
            Parser(
                client_socket, cmd_list, recv_bytes, args, restored_kv_pairs
            ).parse_command()
    except socket.error as ex:
        print(f"Socket error: {ex}")
    finally:
        print(f"[{role}] Closing client socket: {client_socket}")
        client_socket.close()


class ReplicationHandshake:
    """
    Class to handle the replication handshake process.
    """

    def __init__(self, args: Namespace) -> None:
        """
        Initialize the ReplicationHandshake class.

        Args:
            args (Namespace): The command line arguments parsed by argparse.
        """
        self.args = args

    def connect_to_master(self, master_ip: str, master_port: str) -> socket.socket:
        """
        Connect to the master server.

        Args:
            master_ip (str): The IP address of the master server.
            master_port (str): The port number of the master server.

        Returns:
            socket.socket: The socket object connected to the master server.
        """
        try:
            master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            master_socket.connect((master_ip, int(master_port)))
            return master_socket
        except Exception as ex:
            raise Exception(f"Error connecting to master: {ex}")

    def send_and_receive(self, master_socket: socket.socket, message: bytes) -> str:
        """
        Send a command to the master server.

        Args:
            master_socket (socket.socket): The socket object connected to the master server.
            message (bytes): The command to be sent.

        Returns:
            str: The response from the master server.
        """
        master_socket.send(message)
        return master_socket.recv(MAX_BYTES_TO_RECEIVE).decode("utf-8")

    def perform_handshake(self) -> None:
        """
        Perform the replication handshake process.
        """
        if self.args.replicaof:
            (master_ip, master_port) = self.args.replicaof.split(" ")
            master_socket = self.connect_and_ping_master(master_ip, master_port)
            self.perform_replconf_handshake(master_socket)
            self.perform_psync_handshake(master_socket)
        else:
            print("Current server is master. No replication needed..")

    def connect_and_ping_master(
        self, master_ip: str, master_port: str
    ) -> socket.socket:
        """
        Connect to the master server and send a PING command.

        Args:
            master_ip (str): The IP address of the master server.
            master_port (str): The port number of the master server.

        Returns:
            socket.socket: The socket object connected to the master server.
        """
        master_socket = self.connect_to_master(master_ip, master_port)
        try:
            first_handshake_response = self.send_and_receive(
                master_socket, b"*1\r\n$4\r\nPING\r\n"
            )
            print(f"Received response from master: {first_handshake_response}")
            if "PONG" not in first_handshake_response:
                raise Exception(
                    f"Master server did not respond to PING. Response received: {first_handshake_response}"
                )
        except socket.error as e:
            print(f"Failed to connect to master in PING: {e}. Closing socket..")
            master_socket.close()
            raise
        return master_socket

    def perform_replconf_handshake(self, master_socket: socket.socket) -> None:
        """
        Perform the REPLCONF handshake process.

        Args:
            master_s in perform_psync_handshakeocket (socket.socket): The socket object connected to the master server.
        """
        try:
            second_handshake_response = self.send_and_receive(
                master_socket,
                f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(self.args.port))}\r\n{self.args.port}\r\n".encode(
                    "utf-8"
                ),
            )
            if "OK" not in second_handshake_response:
                raise Exception(
                    f"Master server did not respond to REPLCONF. Response received: {second_handshake_response}"
                )
            print("Second handshake response: ", second_handshake_response)
            final_handshake_response = self.send_and_receive(
                master_socket,
                b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n",
            )
            if "OK" not in final_handshake_response:
                raise Exception(
                    f"Master server did not respond to REPLCONF capabilities. Response received: {final_handshake_response}"
                )
            print("Final handshake response: ", final_handshake_response)
            print("Handshake process complete!")
        except socket.error as e:
            print(f"Failed to connect to master in REPLCONF: {e}")
            master_socket.close()
            raise

    def perform_psync_handshake(self, master_socket: socket.socket) -> None:
        """
        Perform the PSYNC handshake process.

        This method sends a PSYNC message to the master server and expects a FULLRESYNC response.
        If the response is not FULLRESYNC, an exception is raised.

        Args:
            master_socket (socket.socket): The socket object connected to the master server.

        Raises:
            Exception: If the response from the master server is not FULLRESYNC.

        Returns:
            None
        """
        try:
            psync_response = self.send_and_receive(
                master_socket, b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
            )
            print("PSYNC RESPONSE: ", psync_response)
            if "FULLRESYNC" in psync_response:
                print(
                    "Received FULLRESYNC from master. Replication handshake complete."
                )
                # TODO: Write the received RDB file to disk
                rdb_file_response = master_socket.recv(MAX_BYTES_TO_RECEIVE)
                print(f"Received RDB file response: {rdb_file_response}")
                last_command = None
                if len(rdb_file_response) > RDB_FILE_SIZE_BYTES:
                    print("Received additional command after RDB file")
                    last_command = rdb_file_response[RDB_FILE_SIZE_BYTES:]

                self.handle_master_commands(master_socket, last_command)
            else:
                raise Exception(
                    f"Expected FULLRESYNC from master. Received: {psync_response}"
                )
        except Exception as e:
            print(f"Failed to connect to master in PSYNC: {e}")
        finally:
            print("Closing master socket in perform_psync_handshake...")
            master_socket.close()

    def handle_master_commands(
        self, master_socket: socket.socket, first_response: Optional[bytes] = None
    ) -> None:
        """
        Handle continuous commands from the master after FULLRESYNC.

        This method listens for commands from the master server after the FULLRESYNC process.
        It receives commands from the master, decodes them, and passes them to the Parser for processing.
        If an error occurs during command handling, the method breaks out of the loop and closes the master socket.

        Args:
            master_socket (socket.socket): The socket object connected to the master server.
            first_response (Optional[bytes]): The first response received from the master, if any.

        Returns:
            None
        """
        print("Listening for commands from master...")
        if first_response:
            command = first_response.decode("utf-8")
            cmd_list = command.split("\r\n")
            Parser(master_socket, cmd_list, first_response, self.args).parse_command()
        while True:
            try:
                command_bytes = master_socket.recv(MAX_BYTES_TO_RECEIVE)
                if not command_bytes:
                    break
                command = command_bytes.decode("utf-8")
                cmd_list = command.split("\r\n")
                print("Received command from master: ", cmd_list)
                Parser(
                    master_socket, cmd_list, command_bytes, self.args
                ).parse_command()
            except socket.error as ex:
                print(f"Socket error in handle_master_commands: {ex}")
                break
            except Exception as e:
                print(f"Exception in handle_master_commands: {e}")
                break
        print("Closing master socket in handle_master_commands...")
        master_socket.close()


class RDBFileParser:
    MAGIC_NUMBER_SLICE = slice(0, 5)
    FILE_VERSION_SLICE = slice(5, 9)
    METADATA_START_SLICE = slice(9, 10)

    DATABASE_START_SLICE = slice(28, 29)
    DATABASE_SIZE_SLICE = slice(30, 31)

    OPCODE_AUX = b"\xfa"
    OPCODE_RESIZEDB = b"\xfb"
    OPCODE_EXPIRETIMEMS = b"\xfc"
    OPCODE_EXPIRETIME = b"\xfd"
    OPCODE_SELECTDB = b"\xfe"
    OPCODE_EOF = b"\xff"

    LENGTH_ENCODING_MAPPING = {
        b"\x00": 6,
        b"\x01": 14,
        b"\x10": 32,  # FIXME: Instruction -> Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
        b"\x11": 64,  # FIXME: Instruction -> The next object is encoded in a special format. The remaining 6 bits indicate the format. May be used to store numbers or String
    }

    def __init__(self, args):
        self.args = args
        self.role = "REPLICA" if args.replicaof else "MASTER"

    def _read_file(self):
        dbpath = Path(self.args.dir) / self.args.dbfilename
        if (dbpath).exists():
            with dbpath.open("rb") as infile:
                return infile.read()
        else:
            print("RDB file not found.")
            return None

    def _verify_header(self, data):
        """
        Verifies the header of the RDB file data.

        This method checks if the initial bytes of the provided data match the expected
        Redis RDB file header. If the header is invalid, it raises an exception.

        Args:
            data (bytes): The binary data of the RDB file.

        Raises:
            Exception: If the RDB file header is invalid.
        """
        if data[self.MAGIC_NUMBER_SLICE] != b"REDIS":
            raise Exception("Invalid RDB file header")
        print(f"[{self.role}] Redis File Version: ", data[self.FILE_VERSION_SLICE])

    def _verify_metadata(self, data):
        """
        Verifies the metadata section of the RDB file data.

        This method checks if the metadata section of the provided data starts with the expected
        Redis RDB file auxiliary opcode. If the metadata header is invalid, it raises an exception.

        Args:
            data (bytes): The binary data of the RDB file.

        Raises:
            Exception: If the metadata header is invalid.
        """
        if data[self.METADATA_START_SLICE] != self.OPCODE_AUX:
            raise Exception(
                f"Invalid metadata header. Expected {self.OPCODE_AUX}. Received: {data[self.METADATA_START_SLICE]}"
            )

        metadata_version_name_size_slice = slice(10, 11)
        (name_num_bits,) = struct.unpack_from(
            "B", data[metadata_version_name_size_slice]
        )
        metadata_version_name_slice = slice(11, 11 + name_num_bits)
        print(
            f"[{self.role}] Metadata Redis Version name: ",
            data[metadata_version_name_slice],
        )

        (version_num_bits,) = struct.unpack_from(
            "B", data[11 + name_num_bits : 12 + name_num_bits]
        )
        metadata_version_val_slice = slice(
            12 + name_num_bits, 12 + name_num_bits + version_num_bits
        )
        print(
            f"[{self.role}] Metadata Redis Version value: ",
            data[metadata_version_val_slice],
        )

    def _read_length(self, data, start_idx):
        """
        Reads a length-encoded integer from the data using Redis RDB file encoding.

        This method implements the length encoding used in Redis RDB files:
        1. If the first byte is less than 0xC0, it represents the length directly.
        2. If the first byte is 0xC0, the next 2 bytes represent the length as a 16-bit integer.
        3. If the first byte is 0xC1, the next 4 bytes represent the length as a 32-bit integer.
        4. Other encodings (0xC2 and above) are not supported in this implementation.

        Args:
            data (bytes): The binary data of the RDB file.
            start_idx (int): The starting index to read from in the data.

        Returns:
            Tuple[int, int]: A tuple containing:
                - The decoded length (int)
                - The new index after reading the length (int)

        Raises:
            Exception: If an unsupported length encoding is encountered.
        """
        first_byte = data[start_idx]
        if first_byte < 0xC0:
            # If the first byte is less than 0xC0, it directly represents the length
            return first_byte, start_idx + 1
        elif first_byte < 0xF0:
            if first_byte == 0xC0:
                # If the first byte is 0xC0, read the next 2 bytes as a 16-bit integer
                return struct.unpack(">H", data[start_idx + 1 : start_idx + 3])[
                    0
                ], start_idx + 3
            elif first_byte == 0xC1:
                # If the first byte is 0xC1, read the next 4 bytes as a 32-bit integer
                return struct.unpack(">I", data[start_idx + 1 : start_idx + 5])[
                    0
                ], start_idx + 5
        # If we reach here, it means we've encountered an unsupported encoding
        raise Exception(f"Unsupported length encoding: {first_byte}")

    def _verify_db_selector(self, data) -> int:
        """
        Extracts the database information from the RDB file data.

        This method locates the SELECTDB opcode in the provided data, verifies the presence
        of the RESIZEDB opcode, and extracts the sizes of the hash table and the expire hash table.

        Args:
            data (bytes): The binary data of the RDB file.

        Returns:
            int: The index in the data after processing the database selector information.

        Raises:
            Exception: If the SELECTDB or RESIZEDB opcodes are not found in the data.
        """
        db_start_idx = data.find(self.OPCODE_SELECTDB)
        if db_start_idx == -1:
            raise Exception("Expected SELECTDB opcode not found in the RDB file.")

        hash_table_info_slice = slice(db_start_idx + 2, db_start_idx + 3)
        if data[hash_table_info_slice] != self.OPCODE_RESIZEDB:
            raise Exception(
                f"Expected RESIZEDB opcode not found in the RDB file. Expected {self.OPCODE_RESIZEDB}. Received: {data[hash_table_info_slice]}"
            )

        hash_table_size, idx = self._read_length(data, db_start_idx + 3)
        print(f"[{self.role}] Size of hash table: ", hash_table_size)

        expire_hash_table_size, idx = self._read_length(data, idx)
        print(f"[{self.role}] Size of expire hash table: ", expire_hash_table_size)

        return idx

    def _extract_kv_pairs(self, data, kv_start_idx):
        """
        Extracts key-value pairs from the RDB file data.

        This method reads the key-value pairs from the provided data starting at the given index.
        It handles different value types and expiry times.

        Args:
            data (bytes): The binary data of the RDB file.
            kv_start_idx (int): The starting index for reading key-value pairs.

        Returns:
            List[Tuple[str, str, Optional[int]]]: A list of tuples containing the key-value pairs and optional expiry time.

        Raises:
            Exception: If an unexpected value type is encountered.
        """
        print(f"[{self.role}] RDB data: {data}")
        kv_pairs = []
        idx = kv_start_idx
        while idx < len(data):
            if data[slice(idx, idx + 1)] == self.OPCODE_EOF:  # End of RDB file
                print(f"[{self.role}] EOF found. Breaking..")
                break

            expiry = None
            if data[slice(idx, idx + 1)] == self.OPCODE_EXPIRETIMEMS:
                idx += 1
                expiry = struct.unpack("<Q", data[idx : idx + 8])[
                    0
                ]  # Already in milliseconds
                idx += 8
            elif data[slice(idx, idx + 1)] == self.OPCODE_EXPIRETIME:
                idx += 1
                expiry = (
                    struct.unpack("<I", data[idx : idx + 4])[0] * 1000
                )  # Convert seconds to milliseconds
                idx += 4
            else:
                print(f"[{self.role}] No expiry found..")

            value_type = data[slice(idx, idx + 1)]
            idx += 1

            key_size, idx = self._read_length(data, idx)
            key = data[idx : idx + key_size].decode("utf-8")
            idx += key_size
            print(f"[{self.role}] Key: {key} Key size: {key_size}")

            if value_type == b"\x00":  # String encoding
                value_size, idx = self._read_length(data, idx)
                value = data[idx : idx + value_size].decode("utf-8", errors="ignore")
                idx += value_size
            elif value_type in range(1, 14):  # List encoding
                # Implement list handling if needed
                raise Exception("Encoding not implemented")
            else:
                value_size, idx = self._read_length(data, idx)
                print(f"[{self.role}] Value size: {value_size}")
                value = data[idx : idx + value_size].decode("utf-8", errors="ignore")
                idx += value_size

            print(f"[{self.role}] Key: {key}, Value: {value}, Expiry: {expiry}")
            if key:  # Only add non-empty keys
                kv_pairs.append((key, value, expiry))

        return kv_pairs

    def parse(self):
        """
        Parses the RDB file to extract key-value pairs.

        This method reads the RDB file, verifies its header and metadata,
        extracts the database selector information, and reads the key-value pairs.

        Returns:
            List[Tuple[str, str]]: A list of tuples containing the key-value pairs.
        """
        data = self._read_file()
        if data:
            self._verify_header(data)
            self._verify_metadata(data)
            kv_start_idx = self._verify_db_selector(data)
            kv_pairs = self._extract_kv_pairs(data, kv_start_idx)
            return kv_pairs


def main():
    """
    The main function of the server. It creates a server socket, listens for incoming connections,
    and starts a new thread to process each connection.

    Args:
        None

    Returns:
        None
    """

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p", "--port", help="Redis server port", default=6379, type=int
    )
    parser.add_argument(
        "-r",
        "--replicaof",
        help="Address of Redis master server. Current server will be the replica of the master",
        default=None,
    )
    parser.add_argument(
        "-d",
        "--dir",
        help="Path to the directory where RDB file is stored",
        default="/tmp/redis-data",
    )
    parser.add_argument(
        "-f", "--dbfilename", help="Name of the RDB file", default="dump.rdb"
    )
    args = parser.parse_args()
    restored_kv_pairs = RDBFileParser(args).parse()
    server_socket = socket.create_server(("localhost", args.port), reuse_port=True)
    print(f"Listening on port {args.port}..")
    server_socket.listen(MAX_NUM_UNACCEPTED_CONN)
    print("Server socket: ", server_socket)
    if args.replicaof:
        Thread(target=ReplicationHandshake(args).perform_handshake).start()
    while True:
        (client_socket, _client_add) = server_socket.accept()
        Thread(
            target=process_request,
            args=(client_socket, _client_add, args, restored_kv_pairs),
        ).start()


if __name__ == "__main__":
    main()
