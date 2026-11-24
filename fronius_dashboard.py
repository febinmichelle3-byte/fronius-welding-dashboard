
import os
import json
import time
from datetime import datetime
from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st
from asyncua import ua
from asyncua.sync import Client


# =========================================================
# 1. CONFIGURATION
# =========================================================

BASE_DIR = Path(__file__).resolve().parent

OPC_UA_URL = os.getenv(
    "FRONIUS_OPC_UA_URL",
    "opc.tcp://10.113.107.109:4840"
)

FRONIUS_USERNAME = os.getenv("FRONIUS_USERNAME", "admin")
JOB_NUMBER_NODE = "ns=1;s=JOBNUMBER"

MQTT_HOST_DEFAULT = "127.0.0.1"
MQTT_PORT_DEFAULT = 1883
MQTT_TOPIC_DEFAULT = "welding/tps320i/live"

INTEGER_TYPES = {
    "SByte", "Byte", "Int16", "UInt16",
    "Int32", "UInt32", "Int64", "UInt64"
}

FLOAT_TYPES = {"Float", "Double"}


# Live monitoring nodes identified in the Fronius scan.
LIVE_NODES = {
    "DISPLAY_STATUS": "ns=1;s=DISPLAY_STATUS",
    "DISPLAY_CURRENT": "ns=1;s=DISPLAY_CURRENT",
    "DISPLAY_VOLTAGE": "ns=1;s=DISPLAY_VOLTAGE",
    "DISPLAY_WFS": "ns=1;s=DISPLAY_WFS",
    "DISPLAY_POWER": "ns=1;s=DISPLAY_POWER",
    "DISPLAY_ENERGY": "ns=1;s=DISPLAY_ENERGY",
    "ACTUAL_CURRENT": "ns=1;s=ACTUAL_CURRENT",
    "ACTUAL_VOLTAGE": "ns=1;s=ACTUAL_VOLTAGE",
    "ACTUAL_WFS": "ns=1;s=ACTUAL_WFS",
    "ACTUAL_POWER": "ns=1;s=ACTUAL_POWER",
    "ACTUAL_WELDINGTIME": "ns=1;s=ACTUAL_WELDINGTIME",
    "PROCESS_ACTIVE": "ns=1;s=PROCESS_ACTIVE",
    "ARCSTABLE": "ns=1;s=ARCSTABLE",
}


# =========================================================
# 2. STREAMLIT CONFIGURATION
# =========================================================

st.set_page_config(
    page_title="Fronius TPS 320i IIoT Dashboard",
    page_icon="⚙️",
    layout="wide"
)

st.title("Fronius TPS 320i IIoT Dashboard")
st.caption(
    "OPC UA | Live Welding Monitoring | Writable Nodes | MQTT"
)


# =========================================================
# 3. OPC UA CONNECTION
# =========================================================

def connect_opcua(endpoint, username, password):

    client = Client(url=endpoint, timeout=15)

    if username:
        client.set_user(username)

    if password:
        client.set_password(password)

    client.connect()

    return client


def disconnect_opcua(client):

    if client is not None:
        try:
            client.disconnect()
        except Exception:
            pass


# =========================================================
# 4. ACCESS LEVEL DECODING
# =========================================================

def has_access_flag(access_value, required_flag):

    if access_value is None:
        return False

    try:
        if required_flag in access_value:
            return True
    except Exception:
        pass

    try:
        return bool(
            int(access_value) & int(required_flag)
        )
    except Exception:
        pass

    try:
        for item in access_value:
            if int(item) == int(required_flag):
                return True
    except Exception:
        pass

    return False


def get_node_access(node):

    try:
        access = node.get_access_level()
    except Exception:
        access = None

    try:
        user_access = node.get_user_access_level()
    except Exception:
        user_access = None

    return {
        "server_read": has_access_flag(
            access, ua.AccessLevel.CurrentRead
        ),
        "server_write": has_access_flag(
            access, ua.AccessLevel.CurrentWrite
        ),
        "user_read": has_access_flag(
            user_access, ua.AccessLevel.CurrentRead
        ),
        "user_write": has_access_flag(
            user_access, ua.AccessLevel.CurrentWrite
        ),
        "access_raw": str(access),
        "user_access_raw": str(user_access)
    }


# =========================================================
# 5. RESOLVE ACTUAL OPC UA VALUE TYPE
# =========================================================

def get_runtime_variant(data_value):

    try:
        variant = data_value.Value

        if variant is None:
            return None, None

        variant_type = variant.VariantType
        actual_value = variant.Value

        while (
            variant_type == ua.VariantType.Variant
            and isinstance(actual_value, ua.Variant)
        ):
            variant_type = actual_value.VariantType
            actual_value = actual_value.Value

        if variant_type != ua.VariantType.Variant:
            return variant_type, actual_value

        if isinstance(actual_value, bool):
            return ua.VariantType.Boolean, actual_value

        if isinstance(actual_value, int):
            return ua.VariantType.Int64, actual_value

        if isinstance(actual_value, float):
            return ua.VariantType.Double, actual_value

        if isinstance(actual_value, str):
            return ua.VariantType.String, actual_value

        return ua.VariantType.Variant, actual_value

    except Exception:
        return None, None


def get_node_datatype(node):

    declared_name = "Unknown"
    declared_id = None

    try:
        declared = node.read_data_type_as_variant_type()
        declared_id = int(declared)
        declared_name = ua.VariantType(declared_id).name
    except Exception:
        pass

    try:
        data_value = node.read_data_value()

        runtime_type, runtime_value = get_runtime_variant(
            data_value
        )

        if runtime_type is not None:

            runtime_name = runtime_type.name

            if runtime_name != "Variant":
                if declared_name == "Variant":
                    display_name = (
                        f"{runtime_name} "
                        "(runtime value; declared Variant)"
                    )
                else:
                    display_name = runtime_name

                return (
                    int(runtime_type),
                    display_name,
                    runtime_name,
                    runtime_value
                )

    except Exception:
        pass

    return (
        declared_id,
        declared_name,
        declared_name,
        None
    )


def safe_read(node):

    try:
        return node.read_value(), ""
    except Exception as exc:
        return None, str(exc)


def stringify(value):

    if value is None:
        return ""

    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, default=str)

    return str(value)


# =========================================================
# 6. LIVE WELDING DATA READING
# =========================================================

def read_live_machine_data(
    endpoint,
    username,
    password
):

    client = None
    live_data = {}

    try:

        client = connect_opcua(
            endpoint,
            username,
            password
        )

        for name, node_id in LIVE_NODES.items():

            try:

                node = client.get_node(node_id)
                value = node.read_value()

                live_data[name] = {
                    "NodeId": node_id,
                    "Value": value,
                    "Error": ""
                }

            except Exception as exc:

                live_data[name] = {
                    "NodeId": node_id,
                    "Value": None,
                    "Error": str(exc)
                }

        live_data["_timestamp"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        live_data["_connection"] = "Connected"

        return live_data

    except Exception as exc:

        return {
            "_connection": "Disconnected",
            "_error": str(exc),
            "_timestamp": datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        }

    finally:

        disconnect_opcua(client)


def format_live_value(value, decimal_places=2):

    if value is None:
        return "N/A"

    if isinstance(value, bool):
        return "Active" if value else "Inactive"

    if isinstance(value, (int, float)):

        number = float(value)

        # Some Fronius nodes return very large placeholder values.
        if abs(number) > 1e30:
            return "N/A (sentinel value)"

        return f"{number:.{decimal_places}f}"

    return str(value)


def get_live_value(live_data, name):

    item = live_data.get(name, {})

    if not isinstance(item, dict):
        return None

    return item.get("Value")


def live_metric(live_data, name, label, unit=""):

    value = get_live_value(live_data, name)

    if value is None:
        display = "N/A"
    else:
        display = format_live_value(value)

        if unit and display != "N/A" and "sentinel" not in display:
            display = f"{display} {unit}"

    st.metric(label, display)


def render_live_monitor(
    endpoint,
    username,
    password,
    auto_refresh
):

    if auto_refresh:

        live_data = read_live_machine_data(
            endpoint,
            username,
            password
        )

        st.session_state.live_monitor_data = live_data

    elif "live_monitor_data" not in st.session_state:

        live_data = read_live_machine_data(
            endpoint,
            username,
            password
        )

        st.session_state.live_monitor_data = live_data

    live_data = st.session_state.live_monitor_data

    if live_data.get("_connection") != "Connected":

        st.error(
            "OPC UA live-monitor connection failed: "
            + str(live_data.get("_error", "Unknown error"))
        )

        return

    st.success("Connected to Fronius OPC UA server")

    st.caption(
        "Last live update: "
        + str(live_data.get("_timestamp", "Unknown"))
    )

    # Welding state
    process_active = get_live_value(
        live_data, "PROCESS_ACTIVE"
    )

    arc_stable = get_live_value(
        live_data, "ARCSTABLE"
    )

    status_col1, status_col2 = st.columns(2)

    with status_col1:

        if process_active is True:
            st.success("Welding Process: ACTIVE")
        elif process_active is False:
            st.info("Welding Process: INACTIVE")
        else:
            st.warning("Welding Process: UNKNOWN")

    with status_col2:

        if arc_stable is True:
            st.success("Arc Stability: STABLE")
        elif arc_stable is False:
            st.info("Arc Stability: NOT STABLE / INACTIVE")
        else:
            st.warning("Arc Stability: UNKNOWN")

    st.divider()

    st.markdown("### Displayed Welding Parameters")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        live_metric(
            live_data,
            "DISPLAY_CURRENT",
            "Display Current",
            "A"
        )

    with col2:
        live_metric(
            live_data,
            "DISPLAY_VOLTAGE",
            "Display Voltage",
            "V"
        )

    with col3:
        live_metric(
            live_data,
            "DISPLAY_WFS",
            "Display Wire Feed Speed",
            "m/min"
        )

    with col4:
        live_metric(
            live_data,
            "DISPLAY_POWER",
            "Display Power",
            "kW"
        )

    col5, col6 = st.columns(2)

    with col5:
        live_metric(
            live_data,
            "DISPLAY_ENERGY",
            "Display Energy"
        )

    with col6:
        live_metric(
            live_data,
            "DISPLAY_STATUS",
            "Display Status"
        )

    st.divider()

    st.markdown("### Actual Welding Parameters")

    st.caption(
        "These are separate ACTUAL_* nodes. Their values must be "
        "observed during welding to establish when and how they update."
    )

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        live_metric(
            live_data,
            "ACTUAL_CURRENT",
            "Actual Current",
            "A"
        )

    with col2:
        live_metric(
            live_data,
            "ACTUAL_VOLTAGE",
            "Actual Voltage",
            "V"
        )

    with col3:
        live_metric(
            live_data,
            "ACTUAL_WFS",
            "Actual Wire Feed Speed",
            "m/min"
        )

    with col4:
        live_metric(
            live_data,
            "ACTUAL_POWER",
            "Actual Power",
            "kW"
        )

    col5 = st.columns(1)[0]

    with col5:
        live_metric(
            live_data,
            "ACTUAL_WELDINGTIME",
            "Actual Welding Time",
            "s"
        )

    st.divider()

    st.markdown("### Raw Live OPC UA Values")

    raw_rows = []

    for name, node_id in LIVE_NODES.items():

        item = live_data.get(name, {})

        if not isinstance(item, dict):
            item = {}

        raw_rows.append({
            "Parameter": name,
            "NodeId": node_id,
            "Value": stringify(item.get("Value")),
            "Read Error": item.get("Error", "")
        })

    raw_df = pd.DataFrame(raw_rows)

    st.dataframe(
        raw_df,
        use_container_width=True,
        hide_index=True
    )

    st.download_button(
        "Download Live Monitoring CSV",
        raw_df.to_csv(index=False).encode("utf-8"),
        file_name="fronius_live_monitor.csv",
        mime="text/csv",
        key="download_live_monitor"
    )


# =========================================================
# 7. SCAN ALL VARIABLE NODES
# =========================================================

def scan_all_nodes(client):

    objects = client.get_objects_node()

    queue = [(objects, "Objects", 0)]

    visited = set()
    all_nodes = []

    while queue:

        parent, parent_path, depth = queue.pop(0)

        if depth > 20:
            continue

        try:
            children = parent.get_children()
        except Exception:
            continue

        for child in children:

            node_id = child.nodeid.to_string()

            if node_id in visited:
                continue

            visited.add(node_id)

            try:
                browse_name = child.read_browse_name().Name
            except Exception:
                browse_name = node_id

            path = parent_path + "/" + str(browse_name)

            try:
                node_class = child.read_node_class()
            except Exception:
                node_class = None

            if node_class == ua.NodeClass.Variable:

                access = get_node_access(child)

                type_id, display_type, runtime_type, runtime_value = (
                    get_node_datatype(child)
                )

                value, read_error = safe_read(child)

                writable = (
                    access["server_write"]
                    and access["user_write"]
                )

                all_nodes.append({

                    "Node Name": str(browse_name),
                    "Browse Path": path,
                    "NodeId": node_id,

                    "Data Type": display_type,
                    "Runtime Type": runtime_type,
                    "Data Type ID": type_id,

                    "Current Value": stringify(value),

                    "Server Read": access["server_read"],
                    "Server Write": access["server_write"],
                    "User Read": access["user_read"],
                    "User Write": access["user_write"],

                    "Writable": writable,

                    "AccessLevel": access["access_raw"],
                    "UserAccessLevel": access["user_access_raw"],

                    "Read Error": read_error

                })

            queue.append(
                (child, path, depth + 1)
            )

    writable_nodes = [
        item for item in all_nodes
        if item["Writable"]
    ]

    return all_nodes, writable_nodes


# =========================================================
# 8. VALUE CONVERSION
# =========================================================

def convert_value(value, datatype):

    if datatype == "Boolean":

        text = str(value).strip().lower()

        if text in ("true", "1", "yes"):
            return True

        if text in ("false", "0", "no"):
            return False

        raise ValueError("Enter True or False.")

    if datatype in INTEGER_TYPES:

        number = float(value)

        if not number.is_integer():
            raise ValueError(
                "This node requires a whole number."
            )

        return int(number)

    if datatype in FLOAT_TYPES:
        return float(value)

    if datatype == "String":
        return str(value)

    raise ValueError(
        f"Unsupported write data type: {datatype}"
    )


# =========================================================
# 9. WRITE INDIVIDUAL NODE
# =========================================================

def write_node(
    endpoint,
    username,
    password,
    node_id,
    new_value
):

    client = None

    try:

        client = connect_opcua(
            endpoint,
            username,
            password
        )

        node = client.get_node(node_id)

        access = get_node_access(node)

        if not access["server_write"]:
            raise PermissionError(
                "Server AccessLevel does not permit writing."
            )

        if not access["user_write"]:
            raise PermissionError(
                "UserAccessLevel does not permit writing."
            )

        before = node.read_value()

        type_id, display_type, runtime_type, runtime_value = (
            get_node_datatype(node)
        )

        converted = convert_value(
            new_value,
            runtime_type
        )

        variant_type = ua.VariantType(type_id)

        node.write_value(
            ua.Variant(
                converted,
                variant_type
            )
        )

        time.sleep(1)

        after = node.read_value()

        verified = (
            type(after) == type(converted)
            and after == converted
        )

        return {

            "NodeId": node_id,
            "Declared/Display Type": display_type,
            "Write Type": runtime_type,

            "Before": stringify(before),
            "Requested": stringify(converted),
            "After": stringify(after),

            "Verified": verified

        }

    finally:
        disconnect_opcua(client)


# =========================================================
# 10. JOB NUMBER SWITCHING
# =========================================================

def switch_job(
    endpoint,
    username,
    password,
    job_number
):

    client = None

    try:

        client = connect_opcua(
            endpoint,
            username,
            password
        )

        job_node = client.get_node(
            JOB_NUMBER_NODE
        )

        access = get_node_access(job_node)

        if not (
            access["server_write"]
            and access["user_write"]
        ):
            raise PermissionError(
                "JOBNUMBER is not writable."
            )

        before = float(job_node.read_value())
        target = float(job_number)

        if target <= 0:
            raise ValueError(
                "Job number must be greater than zero."
            )

        job_node.write_value(
            ua.Variant(
                target,
                ua.VariantType.Float
            )
        )

        time.sleep(1)

        after = float(job_node.read_value())

        verified = abs(after - target) < 0.0001

        return {
            "NodeId": JOB_NUMBER_NODE,
            "Data Type": "Float",
            "Before": before,
            "Requested": target,
            "After": after,
            "Verified": verified
        }

    finally:
        disconnect_opcua(client)


# =========================================================
# 11. REFRESH LIVE VALUES
# =========================================================

def refresh_values(
    endpoint,
    username,
    password,
    nodes
):

    client = None
    updated = []

    try:

        client = connect_opcua(
            endpoint,
            username,
            password
        )

        for item in nodes:

            node = client.get_node(item["NodeId"])

            value, error = safe_read(node)

            new_item = item.copy()

            new_item["Current Value"] = stringify(value)
            new_item["Read Error"] = error

            type_id, display_type, runtime_type, runtime_value = (
                get_node_datatype(node)
            )

            new_item["Data Type"] = display_type
            new_item["Runtime Type"] = runtime_type
            new_item["Data Type ID"] = type_id

            updated.append(new_item)

        return updated

    finally:
        disconnect_opcua(client)


# =========================================================
# 12. MQTT PUBLISHING
# =========================================================

def publish_mqtt(host, port, topic, payload):

    import paho.mqtt.client as mqtt

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2
    )

    client.connect(host, int(port), 30)
    client.loop_start()

    try:

        message = client.publish(
            topic,
            json.dumps(payload, default=str),
            qos=0
        )

        message.wait_for_publish(timeout=5)

        if not message.is_published():
            raise RuntimeError(
                "MQTT message was not confirmed as published."
            )

    finally:

        client.loop_stop()
        client.disconnect()


# =========================================================
# 13. EXCEL EXPORT
# =========================================================

def excel_export(dataframe):

    output = BytesIO()

    with pd.ExcelWriter(
        output,
        engine="openpyxl"
    ) as writer:

        dataframe.to_excel(
            writer,
            index=False,
            sheet_name="Writable Nodes"
        )

    output.seek(0)

    return output.getvalue()


# =========================================================
# 14. SESSION STATE
# =========================================================

if "all_nodes" not in st.session_state:
    st.session_state.all_nodes = []

if "writable_nodes" not in st.session_state:
    st.session_state.writable_nodes = []

if "scan_time" not in st.session_state:
    st.session_state.scan_time = None

if "live_monitor_data" not in st.session_state:
    st.session_state.live_monitor_data = {}


# =========================================================
# 15. SIDEBAR
# =========================================================

st.sidebar.header("OPC UA Configuration")

endpoint = st.sidebar.text_input(
    "Fronius OPC UA Endpoint",
    OPC_UA_URL
)

username = st.sidebar.text_input(
    "Username",
    FRONIUS_USERNAME
)

password = st.sidebar.text_input(
    "Password",
    value=os.getenv("FRONIUS_PASSWORD", ""),
    type="password"
)

st.sidebar.divider()

st.sidebar.header("MQTT Configuration")

mqtt_host = st.sidebar.text_input(
    "MQTT Broker",
    MQTT_HOST_DEFAULT
)

mqtt_port = st.sidebar.number_input(
    "MQTT Port",
    min_value=1,
    max_value=65535,
    value=MQTT_PORT_DEFAULT
)

mqtt_topic = st.sidebar.text_input(
    "MQTT Topic",
    MQTT_TOPIC_DEFAULT
)


# =========================================================
# 16. CONNECT AND SCAN
# =========================================================

col1, col2 = st.columns(2)

with col1:

    scan_button = st.button(
        "Connect and Scan All Nodes",
        type="primary",
        use_container_width=True
    )

with col2:

    refresh_button = st.button(
        "Refresh All Scanned Values",
        use_container_width=True
    )


if scan_button:

    client = None

    try:

        with st.spinner(
            "Connecting and scanning Fronius OPC UA..."
        ):

            client = connect_opcua(
                endpoint,
                username,
                password
            )

            all_nodes, writable_nodes = scan_all_nodes(
                client
            )

            st.session_state.all_nodes = all_nodes
            st.session_state.writable_nodes = writable_nodes

            st.session_state.scan_time = datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        st.success(
            f"Scan complete: {len(all_nodes)} variable nodes; "
            f"{len(writable_nodes)} writable nodes."
        )

    except Exception as exc:

        st.error(f"Connection or scanning failed: {exc}")

    finally:

        disconnect_opcua(client)


if refresh_button:

    if not st.session_state.all_nodes:

        st.warning("Scan the machine first.")

    else:

        try:

            with st.spinner("Refreshing scanned machine values..."):

                updated = refresh_values(
                    endpoint,
                    username,
                    password,
                    st.session_state.all_nodes
                )

                st.session_state.all_nodes = updated

                writable_ids = {
                    item["NodeId"]
                    for item in st.session_state.writable_nodes
                }

                st.session_state.writable_nodes = [
                    item for item in updated
                    if item["NodeId"] in writable_ids
                ]

            st.success("All scanned values refreshed.")

        except Exception as exc:

            st.error(f"Refresh failed: {exc}")


# =========================================================
# 17. SUMMARY
# =========================================================

all_nodes = st.session_state.all_nodes
writable_nodes = st.session_state.writable_nodes

m1, m2, m3 = st.columns(3)

m1.metric("Total Variable Nodes", len(all_nodes))
m2.metric("Writable Nodes", len(writable_nodes))
m3.metric(
    "Other Nodes",
    len(all_nodes) - len(writable_nodes)
)

if st.session_state.scan_time:
    st.caption("Last full scan: " + st.session_state.scan_time)


# =========================================================
# 18. DASHBOARD TABS
# =========================================================

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Live Machine Data",
    "All Writable Nodes",
    "Write Node",
    "Job Switching",
    "MQTT"
])


# =========================================================
# TAB 1 - LIVE MACHINE DATA
# =========================================================

with tab1:

    st.subheader("Live Fronius Welding Monitor")

    st.info(
        "This monitor reads the selected Fronius OPC UA nodes directly. "
        "It does not depend on the full-node scan."
    )

    auto_refresh = st.checkbox(
        "Enable automatic live refresh (every 2 seconds)",
        value=False,
        key="live_auto_refresh"
    )

    if auto_refresh:
        st.caption(
            "Automatic refresh is enabled. Keep the dashboard open "
            "while observing the machine."
        )

    else:
        st.caption(
            "Automatic refresh is disabled. Use the button below "
            "to request a new reading."
        )

    manual_refresh = st.button(
        "Read Live Machine Values Now",
        key="manual_live_refresh"
    )

    if manual_refresh:
        st.session_state.live_monitor_data = read_live_machine_data(
            endpoint,
            username,
            password
        )

    if hasattr(st, "fragment"):

        @st.fragment(run_every="2s")
        def live_monitor_fragment(
            endpoint_value,
            username_value,
            password_value,
            auto_value
        ):

            if auto_value:

                render_live_monitor(
                    endpoint_value,
                    username_value,
                    password_value,
                    True
                )

            else:

                render_live_monitor(
                    endpoint_value,
                    username_value,
                    password_value,
                    False
                )

        live_monitor_fragment(
            endpoint,
            username,
            password,
            auto_refresh
        )

    else:

        if auto_refresh:
            st.warning(
                "Automatic refresh requires a newer Streamlit version. "
                "Run: python -m pip install --upgrade streamlit"
            )

        render_live_monitor(
            endpoint,
            username,
            password,
            False
        )

    st.divider()

    st.subheader("All Scanned Machine Variables")

    if not all_nodes:

        st.info("Connect and scan the machine to display all variables.")

    else:

        search = st.text_input(
            "Search machine parameters",
            key="live_search"
        )

        filtered = [
            item for item in all_nodes
            if search.lower() in (
                item["Node Name"]
                + item["NodeId"]
                + item["Browse Path"]
            ).lower()
        ]

        live_df = pd.DataFrame(filtered)

        st.dataframe(
            live_df,
            use_container_width=True,
            hide_index=True
        )

        st.download_button(
            "Download All Scanned Data CSV",
            live_df.to_csv(index=False).encode("utf-8"),
            file_name="fronius_live_data.csv",
            mime="text/csv"
        )


# =========================================================
# TAB 2 - ALL WRITABLE NODES
# =========================================================

with tab2:

    st.subheader("All Writable OPC UA Nodes")

    st.warning(
        "Server write permission does not guarantee that a node "
        "is a welding parameter or that a particular value is safe."
    )

    if not writable_nodes:

        st.info("Run the OPC UA scan first.")

    else:

        search = st.text_input(
            "Search writable nodes",
            key="writable_search"
        )

        filtered = [
            item for item in writable_nodes
            if search.lower() in (
                item["Node Name"]
                + item["NodeId"]
                + item["Browse Path"]
                + item["Data Type"]
            ).lower()
        ]

        writable_df = pd.DataFrame(filtered)

        st.metric(
            "Writable nodes found",
            len(writable_nodes)
        )

        st.dataframe(
            writable_df,
            use_container_width=True,
            hide_index=True
        )

        st.download_button(
            "Download Writable Nodes CSV",
            writable_df.to_csv(index=False).encode("utf-8"),
            file_name="Fronius_Writable_Nodes.csv",
            mime="text/csv"
        )

        st.download_button(
            "Download Writable Nodes Excel",
            excel_export(pd.DataFrame(writable_nodes)),
            file_name="Fronius_Writable_Nodes.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )


# =========================================================
# TAB 3 - INDIVIDUAL WRITABLE NODE CONTROL
# =========================================================

with tab3:

    st.subheader("Individual Writable Node Control")

    st.warning(
        "Only modify a node after confirming its meaning, units, "
        "valid range, and machine-side effect. Test with welding stopped."
    )

    if not writable_nodes:

        st.info("Scan the Fronius machine first.")

    else:

        labels = [
            (
                f'{item["Browse Path"]} | '
                f'{item["Data Type"]} | '
                f'Value: {item["Current Value"]}'
            )
            for item in writable_nodes
        ]

        selected = st.selectbox(
            "Select writable node",
            labels
        )

        index = labels.index(selected)
        selected_node = writable_nodes[index]

        st.write("**NodeId:**", selected_node["NodeId"])
        st.write("**Declared/Display Type:**", selected_node["Data Type"])
        st.write("**Runtime Type:**", selected_node["Runtime Type"])
        st.write("**Current Value:**", selected_node["Current Value"])

        datatype = selected_node["Runtime Type"]

        if datatype == "Boolean":

            default_value = (
                selected_node["Current Value"].lower() == "true"
            )

            new_value = st.checkbox(
                "New Boolean value",
                value=default_value
            )

        elif datatype in INTEGER_TYPES or datatype in FLOAT_TYPES:

            new_value = st.text_input(
                "New numeric value",
                value=selected_node["Current Value"]
            )

        elif datatype == "String":

            new_value = st.text_input(
                "New string value",
                value=selected_node["Current Value"]
            )

        else:

            st.error(
                f"Unsupported or unresolved write type: {datatype}. "
                "The node remains visible, but writing is disabled."
            )

            new_value = None

        confirmation = st.checkbox(
            "I have verified this node, its valid range and the requested value.",
            key="write_confirmation"
        )

        if st.button(
            "Write Selected Node",
            type="primary",
            disabled=(
                new_value is None
                or not confirmation
            )
        ):

            try:

                result = write_node(
                    endpoint,
                    username,
                    password,
                    selected_node["NodeId"],
                    new_value
                )

                if result["Verified"]:

                    st.success(
                        "Write and read-back verification successful."
                    )

                else:

                    st.warning(
                        "Write completed, but read-back does not match."
                    )

                st.json(result)

            except Exception as exc:

                st.error(f"Write failed: {exc}")


# =========================================================
# TAB 4 - JOB SWITCHING
# =========================================================

with tab4:

    st.subheader("Stored Job Switching")

    st.write("**JOBNUMBER NodeId:**", JOB_NUMBER_NODE)
    st.write("**Expected Data Type:** Float")

    job_number = st.number_input(
        "Enter target job number",
        min_value=1.0,
        value=30.0,
        step=1.0
    )

    st.warning(
        "Confirm that the requested job exists. A changed JOBNUMBER "
        "value does not prove that the machine activated the job."
    )

    confirmation = st.checkbox(
        "Machine is idle and target job number is confirmed.",
        key="job_confirmation"
    )

    if st.button(
        "Switch Machine Job",
        type="primary",
        disabled=not confirmation
    ):

        try:

            result = switch_job(
                endpoint,
                username,
                password,
                job_number
            )

            if result["Verified"]:

                st.success(
                    f"JOBNUMBER changed from {result['Before']} "
                    f"to {result['After']}."
                )

                st.info(
                    "Verify the active job on the Fronius machine "
                    "display or SmartManager."
                )

            else:

                st.error("JOBNUMBER read-back verification failed.")

            st.json(result)

        except Exception as exc:

            st.error(f"Job switching failed: {exc}")


# =========================================================
# TAB 5 - MQTT
# =========================================================

with tab5:

    st.subheader("MQTT Data Publishing")

    st.write(f"Broker: {mqtt_host}:{mqtt_port}")
    st.write("Topic:", mqtt_topic)

    if st.button(
        "Publish All Machine Data to MQTT",
        type="primary"
    ):

        if not all_nodes:

            st.error("Scan the Fronius machine first.")

        else:

            payload = {
                "machine": "Fronius TPS 320i",
                "timestamp": datetime.now().isoformat(),
                "variables": {
                    item["NodeId"]: {
                        "name": item["Node Name"],
                        "browse_path": item["Browse Path"],
                        "data_type": item["Data Type"],
                        "runtime_type": item["Runtime Type"],
                        "value": item["Current Value"]
                    }
                    for item in all_nodes
                }
            }

            try:

                publish_mqtt(
                    mqtt_host,
                    mqtt_port,
                    mqtt_topic,
                    payload
                )

                st.success("Machine data published to MQTT.")

            except Exception as exc:

                st.error(f"MQTT publishing failed: {exc}")


# =========================================================
# 19. FOOTER
# =========================================================

st.divider()

st.caption(
    "Fronius TPS 320i | OPC UA IIoT Gateway | "
    "Validate all write operations against machine documentation."
)
