//Package configmanager, reads JSON configuration with an arbitrary structure
package configmanager

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/google/uuid"
	"github.com/quadtrix/aesengine"
	"github.com/quadtrix/audit"
	"github.com/quadtrix/basicqueue"
	"github.com/quadtrix/servicelogger"
)

type ConfigFileType int

const (
	CFT_YAML ConfigFileType = 1 // Not supported
	CFT_JSON ConfigFileType = 2
)

// Configuration defines the Configuration structure. It has no exposed properties
type Configuration struct {
	ready            bool
	fileread         bool
	unmarshaled      bool
	monitoring       bool
	filename         string
	searchpaths      []string
	filetype         ConfigFileType
	reloadOnChange   bool
	fileWatcher      *fsnotify.Watcher
	lastMonitorEvent time.Time
	realfilename     string
	writable         bool
	fileContent      []byte
	jsonConfigMap    map[string]interface{}
	queue            *basicqueue.BasicQueue
	slog             *servicelogger.Logger
	queue_identifier string
	aesengine        aesengine.AESEngine
	auditing         *audit.Audit
}

// New creates a new Configuration object
func New(slog *servicelogger.Logger, queue *basicqueue.BasicQueue) (cfg Configuration, err error) {
	cfg.queue_identifier = fmt.Sprintf("configmanager_%s", uuid.New().String())
	cfg.ready = false
	cfg.fileread = false
	cfg.unmarshaled = false
	cfg.reloadOnChange = false
	cfg.filetype = -1
	cfg.queue = queue
	if err != nil {
		return cfg, err
	}
	err = cfg.queue.RegisterProducer(cfg.queue_identifier)
	if err != nil {
		return cfg, err
	}
	cfg.fileWatcher, err = fsnotify.NewWatcher()
	cfg.slog = slog
	if err != nil {
		return cfg, err
	}
	cfg.slog.LogTrace("New", "configmanager", "Created configmanager object")
	return cfg, nil
}

// SetFilename defines the filename (without extension) to use
func (cfg *Configuration) SetFilename(filename string) {
	cfg.filename = filename
	//fmt.Println(fmt.Sprintf("TRACE: cfg.filename: %s, cfg.filetype: %d, cfg.searchpaths: %v", cfg.filename, cfg.filetype, cfg.searchpaths))
	if cfg.filename != "" && len(cfg.searchpaths) > 0 && cfg.filetype > 0 {
		cfg.ready = true
	}
}

// AddSearchPath adds a search path to the list of paths to search for the configuration file
func (cfg *Configuration) AddSearchPath(searchpath string) {
	cfg.searchpaths = append(cfg.searchpaths, searchpath)
	//fmt.Println(fmt.Sprintf("TRACE: cfg.filename: %s, cfg.filetype: %d, cfg.searchpaths: %v", cfg.filename, cfg.filetype, cfg.searchpaths))
	if cfg.filename != "" && len(cfg.searchpaths) > 0 && cfg.filetype > 0 {
		cfg.ready = true
	}
}

// SetFiletype sets the file type of the configuration file. See ConfigFileType for value definitions
func (cfg *Configuration) SetFiletype(filetype ConfigFileType) {
	cfg.filetype = filetype
	//fmt.Println(fmt.Sprintf("TRACE: cfg.filename: %s, cfg.filetype: %d, cfg.searchpaths: %v", cfg.filename, cfg.filetype, cfg.searchpaths))
	if cfg.filename != "" && len(cfg.searchpaths) > 0 && cfg.filetype > 0 {
		cfg.ready = true
	}
}

func (cfg *Configuration) removeWatcher() {
	cfg.fileWatcher.Remove(cfg.realfilename)
	cfg.monitoring = false
	cfg.slog.LogTrace("removeWatcher", "configmanager", fmt.Sprintf("Removed file watcher for %s", cfg.realfilename))
}

func (cfg *Configuration) addWatcher() {
	cfg.fileWatcher.Add(cfg.realfilename)
	cfg.monitoring = true
	cfg.slog.LogTrace("addWatcher", "configmanager", fmt.Sprintf("Adding file watcher for %s", cfg.realfilename))
}

func (cfg *Configuration) startMonitoring() {
	cfg.slog.LogTrace("startMonitoring", "configmanager", fmt.Sprintf("Starting fsnotify for %s", cfg.realfilename))
	cfg.lastMonitorEvent = time.Now()
	done := make(chan bool)
	go func() {
		defer close(done)

		for {
			select {
			case event, ok := <-cfg.fileWatcher.Events:
				if !ok {
					return
				}
				if time.Now().Sub(cfg.lastMonitorEvent) > time.Second {
					cfg.slog.LogTrace("startMonitoring", "configmanager", fmt.Sprintf("Received fsnotify event: %v", event))
					cfg.queue.AddMessage(cfg.queue_identifier, fmt.Sprintf("EVENT: %s;%s", event.Name, event.Op))
					cfg.lastMonitorEvent = time.Now()
					time.Sleep(time.Second) // This sleep is necessary to make sure the file is fully readable
					err := cfg.reReadConfiguration()
					if err != nil {
						cfg.slog.LogError("startMonitoring", "configmanager", fmt.Sprintf("Failed to reload configuration after fsnotify event: %s", err.Error()))
					}
				}
			case err, ok := <-cfg.fileWatcher.Errors:
				if !ok {
					return
				}
				cfg.queue.AddMessage(cfg.queue_identifier, fmt.Sprintf("ERROR: %s", err))
			}
		}
	}()
	<-done
}

// SetMonitorForChange sets the monitoring flag. If set to true, the configuration file will be monitored for changes and reloaded when it changes
func (cfg *Configuration) SetMonitorForChange(monitor bool) {
	cfg.reloadOnChange = monitor
	if cfg.monitoring {
		if !cfg.reloadOnChange {
			cfg.removeWatcher()
		}
	} else {
		if cfg.reloadOnChange {
			cfg.addWatcher()
			go cfg.startMonitoring()
		}
	}
}

func (cfg Configuration) getFileExtensions() []string {
	switch cfg.filetype {
	case 1: //CFT_YAML
		return []string{"yml", "yaml"}
	case 2: //CFT_JSON
		return []string{"json"}
	default:
		return []string{}
	}
}

func (cfg *Configuration) findConfigFile() (err error) {
	exts := cfg.getFileExtensions()
	searches := ""
	for _, spath := range cfg.searchpaths {
		for _, ext := range exts {
			if _, err = os.Stat(fmt.Sprintf("%s/%s.%s", spath, cfg.filename, ext)); err == nil {
				cfg.slog.LogTrace("findConfigFile", "configmanager", fmt.Sprintf("Found configuration file at %s/%s.%s", spath, cfg.filename, ext))
				cfg.realfilename = fmt.Sprintf("%s/%s.%s", spath, cfg.filename, ext)
				return nil
			} else {
				searches = fmt.Sprintf("%s, %s/%s.%s", searches, spath, cfg.filename, ext)
			}
		}
	}
	return errors.New(fmt.Sprintf("configuration file not found, searched for paths %s", searches))
}

func appendMaps(orgMap map[string]interface{}, subMap map[string]interface{}) (newMap map[string]interface{}, err error) {

	for key, value := range subMap {
		if orgMap[key] != nil {
			return orgMap, errors.New(fmt.Sprintf("key %s is not unique", key))
		}
		orgMap[key] = value
	}
	return orgMap, nil
}

func (cfg *Configuration) SaveEncryptionKey(key string, encryptionKey []byte, encryptionNonce []byte) (err error) {
	keyKey := "key"
	nonceKey := "nonce"
	b64EncodedKey := base64.StdEncoding.EncodeToString(encryptionKey)
	b64EncodedNonce := base64.StdEncoding.EncodeToString(encryptionNonce)
	if cfg.keyExists(key) {
		err = cfg.setJson(fmt.Sprintf("%s.%s", key, keyKey), b64EncodedKey)
		if err != nil {
			return err
		}
		err = cfg.setJson(fmt.Sprintf("%s.%s", key, nonceKey), b64EncodedNonce)
		if err != nil {
			return err
		}
		return nil
	}
	newMapItems := map[string]string{keyKey: string(b64EncodedKey), nonceKey: string(b64EncodedNonce)}
	newMap := map[string]interface{}{key: newMapItems}
	cfg.jsonConfigMap, err = appendMaps(cfg.jsonConfigMap, newMap)
	if err != nil {
		return err
	}
	if cfg.auditing != nil {
		cfg.auditing.AuditLog(time.Now().Format("2006-01-02 15:04:05"), "configmanager", "store encryption key/nonce", "encryption.key and encryption.nonce are added to the in-memory configuration", audit.AU_SYSTEM)
	}
	return nil
}

// ReadConfiguration searches for the configuration file in the defined search paths, reads and unmarshals the file
func (cfg *Configuration) ReadConfiguration() (err error) {
	if !cfg.ready {
		return errors.New(fmt.Sprintf("not ready. configmanager setup is incomplete: filename: %s, filetype: %d, search paths: %v", cfg.filename, cfg.filetype, cfg.searchpaths))
	}
	err = cfg.findConfigFile()
	if err != nil {
		return err
	}
	cfg.fileContent, err = ioutil.ReadFile(cfg.realfilename)
	if err != nil {
		return errors.New(fmt.Sprintf("unable to read configuration file %s, %s", cfg.realfilename, err.Error()))
	}
	cfg.slog.LogTrace("ReadConfiguration", "configmanager", fmt.Sprintf("Loaded file contents from %s", cfg.realfilename))
	cfg.fileread = true
	var f interface{}
	switch cfg.filetype {
	case 1: //CFT_YAML
		err = errors.New("not supported")
	case 2: //CFT_JSON
		err = json.Unmarshal(cfg.fileContent, &f)
	}
	if err != nil {
		return errors.New(fmt.Sprintf("unable to unmarshal configuration file %s, %s", cfg.realfilename, err.Error()))
	}
	cfg.slog.LogTrace("ReadConfiguration", "configmanager", fmt.Sprintf("Unmarshaled file contents from %s", cfg.realfilename))
	cfg.jsonConfigMap = f.(map[string]interface{})
	cfg.unmarshaled = true
	// Initialize Encryption engine from the "encryption" key in the configuration
	if cfg.keyExists("encryption") {
		b64EncodedKey := cfg.GetString("encryption.key")
		b64EncodedNonce := cfg.GetString("encryption.nonce")
		b64DecodedKey, err := base64.StdEncoding.DecodeString(b64EncodedKey)
		if err != nil {
			cfg.slog.LogError("ReadConfiguration", "configmanager", fmt.Sprintf("Unable to decode encryption key: %s", err.Error()))
			return err
		}
		b64DecodedNonce, err := base64.StdEncoding.DecodeString(b64EncodedNonce)
		if err != nil {
			cfg.slog.LogError("ReadConfiguration", "configmanager", fmt.Sprintf("Unable to decode encryption nonce: %s", err.Error()))
			return err
		}
		cfg.aesengine, err = aesengine.NewWithKey(b64DecodedKey, b64DecodedNonce)
		if err != nil {
			cfg.slog.LogError("ReadConfiguration", "configmanager", fmt.Sprintf("Unable to initialize encryption subsystem: %s", err.Error()))
			return err
		}
	} else {
		cfg.aesengine, err = aesengine.New()
		err = cfg.SaveEncryptionKey("encryption", cfg.aesengine.Key, cfg.aesengine.Nonce)
		if err != nil {
			cfg.slog.LogError("ReadConfiguration", "configmanager", fmt.Sprintf("Unable to save encryption key. Encryption will not be reproducable. Cause: %s", err.Error()))
			return err
		}
		cfg.Write("")
	}
	if cfg.reloadOnChange {
		cfg.addWatcher()
	}
	return nil
}

func (cfg *Configuration) reReadConfiguration() (err error) {
	if cfg.auditing != nil {
		cfg.auditing.AuditLog(time.Now().Format("2006-01-02 15:04:05"), "configmanager", "config re-read", fmt.Sprintf("The configuration file %s has changed outside of Delta and is re-read by the application", cfg.realfilename), audit.AU_SYSTEM)
	}
	cfg.slog.LogTrace("reReadConfiguration", "configmanager", fmt.Sprintf("Re-reading configuration from %s after fsnotify event", cfg.realfilename))
	cfg.fileContent, err = ioutil.ReadFile(cfg.realfilename)
	if err != nil {
		return errors.New(fmt.Sprintf("unable to read configuration file %s, %s", cfg.realfilename, err.Error()))
	}
	cfg.fileread = true
	var f interface{}
	err = json.Unmarshal(cfg.fileContent, &f)
	if err != nil {
		return errors.New(fmt.Sprintf("unable to unmarshal configuration file %s, %s", cfg.realfilename, err.Error()))
	}
	cfg.slog.LogTrace("reReadConfiguration", "configmanager", fmt.Sprintf("Unmarshaled new file contents from %s", cfg.realfilename))

	cfg.jsonConfigMap = f.(map[string]interface{})
	cfg.unmarshaled = true
	return nil
}

// AllSettings returns the map of all the settings read from the configuration as an interface{}
func (cfg Configuration) AllSettings() interface{} {
	if cfg.unmarshaled {
		return cfg.jsonConfigMap
	}
	return nil
}

func (cfg Configuration) findStringKey(key string, cfgmap map[string]interface{}) interface{} {
	for mapkey, value := range cfgmap {
		if mapkey == key {
			return value
		}
	}
	return nil
}

func (cfg Configuration) getJson(key string) interface{} {
	if cfg.unmarshaled {
		// Valid mapstr strings:
		// "keyword": searches for the first occurrence of "keyword" in the map and returns the value
		// "key1.key2": searches the map for the first occurrence of "key1", then searches the submap for "key2" and returns the value
		// "key1[2].key2": searches the map for the first occurrence of "key1", reads the 3rd element if it's an array (or error), then searches the submap for "key2" and returns the value
		// "@key1.key2": searches the map for the first occurrence of "key1.key2" and returns the value
		// The function ALWAYS returns interface{}, if the key isn't found, an empty interface{} is returned

		// Determine key type
		keytype := "simple"
		if strings.Contains(key, ".") {
			if key[0] == '@' {
				key = key[1:]
			} else {
				keytype = "complex"
			}
		}
		switch keytype {
		case "simple":

			return cfg.findStringKey(key, cfg.jsonConfigMap)
		case "complex":
			keyparts := strings.Split(key, ".")
			numkeys := len(keyparts)
			stringMap := cfg.jsonConfigMap
			var foundvalue interface{}
			for i := 0; i < numkeys; i++ {
				// Debugging
				fmt.Println(fmt.Sprintf("Key: %s", keyparts[i]))
				foundvalue = cfg.findStringKey(keyparts[i], stringMap)
				if foundvalue != nil {
					fmt.Println("Found: ", foundvalue)
					if i < numkeys-1 {
						stringMap = foundvalue.(map[string]interface{})
					} else {
						return foundvalue
					}
				}
			}
			return foundvalue
		}
	}
	return nil
}

func transcode(in map[string]string) (out map[string]interface{}) {
	buf := new(bytes.Buffer)
	json.NewEncoder(buf).Encode(in)
	json.NewDecoder(buf).Decode(&out)
	return out
}

// Get returns the value of "key" as an interface{}
func (cfg Configuration) Get(key string) interface{} {
	return cfg.getJson(key)
}

// GetString returns the value of "key" as a string. If the key value is encrypted it is decrypted
func (cfg Configuration) GetString(key string) string {
	intf := cfg.Get(key)
	if intf == nil {
		return ""
	}
	value := fmt.Sprintf("%v", intf)
	if len(value) > 4 {
		if value[:4] == "ENC:" {
			b64Encoded := value[4:]
			b64Decoded, err := base64.StdEncoding.DecodeString(b64Encoded)
			if err != nil {
				cfg.slog.LogError("GetString", "configmanager", fmt.Sprintf("Failed to Base64 decode key %s", key))
			}
			cfg.slog.LogTrace("GetString", "configmanager", fmt.Sprintf("Key %s is encrypted. Decrypting...", key))
			value2, err := cfg.aesengine.Decrypt(b64Decoded)
			if err != nil {
				cfg.slog.LogError("GetString", "configmanager", fmt.Sprintf("Failed to decrypt encrypted string: %s", err.Error()))
				return ""
			}
			//cfg.slog.LogTrace("GetString", "configmanager", fmt.Sprintf("REMOVEME: Decrypted key %s to value: %s", key, string(value2)))
			return string(value2)
		}
	}
	return value
}

func (cfg Configuration) getRawString(key string) (string, error) {
	intf := cfg.Get(key)
	if intf != nil {
		value := fmt.Sprintf("%v", intf)
		return value, nil
	}
	return "", errors.New("key not found")
}

// GetBool returns the value of "key" as a bool
func (cfg Configuration) GetBool(key string) (bool, error) {
	intf := cfg.Get(key)
	return strconv.ParseBool(fmt.Sprintf("%v", intf))
}

// GetInt returns the value of "key" as an int
func (cfg Configuration) GetInt(key string) (int, error) {
	intf := cfg.Get(key)
	return strconv.Atoi(fmt.Sprintf("%v", intf))
}

// GetArray returns the value of "key" as an array
func (cfg Configuration) GetArray(key string) (arr map[string]string, err error) {
	arr = map[string]string{}
	result := cfg.Get(key)
	arrt, ok := result.(map[string]interface{})
	if !ok {
		return map[string]string{}, errors.New(fmt.Sprintf("value of %s is not an array", key))
	}
	for key2, value := range arrt {
		val, ok := value.(string)
		if !ok {
			return map[string]string{}, errors.New(fmt.Sprintf("value of %s.%s is not a string", key, key2))
		}
		arr[key2] = val
	}
	return arr, nil
}

// GetArrayValues returns the values of the array below "key" as an array of strings
func (cfg Configuration) GetArrayValues(key string) (values []string, err error) {
	values = []string{}
	result, err := cfg.GetArray(key)
	if err != nil {
		return values, err
	}
	for _, value := range result {
		values = append(values, value)
	}
	return values, nil
}

// IsEncrypted checks whether a key's value is encrypted. Returns 0 when it's encrypted, 1 when it's not encrypted and -1 if the key does not exist
func (cfg Configuration) IsEncrypted(key string) int8 {
	value, err := cfg.getRawString(key)
	if err != nil {
		return -1
	}
	if len(value) > 4 {
		if value[:4] == "ENC:" {
			return 0
		}
		return 1
	}
	return 1
}

func (cfg Configuration) keyExists(key string) bool {
	if cfg.findStringKey(key, cfg.jsonConfigMap) == nil {
		return false
	}
	return true
}

func (cfg *Configuration) setStringKey(key string, value string, stringmap map[string]interface{}) (newmap map[string]interface{}, err error) {
	newmap = stringmap
	newmap[key] = value
	return newmap, nil
}

func (cfg *Configuration) setJson(key string, value string) (err error) {
	keytype := "simple"
	if strings.Contains(key, ".") {
		if key[0] == '@' {
			key = key[1:]
		} else {
			keytype = "complex"
		}
	}
	cfg.slog.LogTrace("setJson", "configmanager", fmt.Sprintf("Key type of key %s is %s", key, keytype))
	switch keytype {
	case "simple":
		cfg.slog.LogTrace("setJson", "configmanager", fmt.Sprintf("Directly setting simple key %s to value %s", key, value))
		newmap, err := cfg.setStringKey(key, value, cfg.jsonConfigMap)
		if err != nil {
			return errors.New(fmt.Sprintf("saving failed: %s", err.Error()))
		}
		cfg.jsonConfigMap = newmap
		return nil
	case "complex":
		cfg.slog.LogTrace("setJson", "configmanager", fmt.Sprintf("Setting complex key %s to value %s", key, value))
		keyparts := strings.Split(key, ".")
		numkeys := len(keyparts)
		var mapHistory struct {
			keys []string
			maps []map[string]interface{}
		}
		stringMap := cfg.jsonConfigMap
		var returnedMap interface{}
		for i := 0; i < numkeys; i++ {
			cfg.slog.LogTrace("setJson", "configmanager", fmt.Sprintf("Adding (sub)map %v (key: %s) to map history position %d", stringMap, keyparts[i], len(mapHistory.maps)))
			mapHistory.keys = append(mapHistory.keys, keyparts[i])
			mapHistory.maps = append(mapHistory.maps, stringMap)
			returnedMap = cfg.findStringKey(keyparts[i], stringMap)
			if returnedMap != nil {
				cfg.slog.LogTrace("setJson", "configmanager", fmt.Sprintf("(Partial) result returned for partial key %s: %v", keyparts[i], returnedMap))
				stringMap2, ok := returnedMap.(map[string]interface{})
				if !ok {
					// This is a value, not a map
					cfg.slog.LogTrace("setJson", "configmanager", fmt.Sprintf("Value found for partial key %s, replacing value with %s", keyparts[i], value))
					returnedMap, err = cfg.setStringKey(keyparts[i], value, stringMap)
					if err != nil {
						return errors.New(fmt.Sprintf("saving failed: %s", err.Error()))
					} else {
						cfg.slog.LogTrace("setJson", "configmanager", fmt.Sprintf("Returned map after replacement: %v", returnedMap))
					}
					// now re-assemble the returned map into the complete jsonConfigMap
					// Our returned map is the same structure as the last element in the mapHistory.maps array, so let's pop that one:
					mapHistory.maps = mapHistory.maps[:len(mapHistory.maps)-1]
					mapHistory.keys = mapHistory.keys[:len(mapHistory.keys)-1]
					// Now re-assemble the remaining parts in reverse order
					nummaps := len(mapHistory.maps)
					cfg.slog.LogTrace("setJson", "configmanager", fmt.Sprintf("Re-assembling config map from %d traversals", nummaps))
					cfg.slog.LogTrace("setJson", "configmanager", fmt.Sprintf("Re-assembling map fragments %d (%v) and returnedMap (%v)", nummaps-1, mapHistory.maps[nummaps-1], returnedMap))
					mapHistory.maps[nummaps-1][mapHistory.keys[nummaps-1]] = returnedMap.(map[string]interface{})
					if nummaps-1 > 0 {
						for j := nummaps - 2; j >= 0; j-- {
							cfg.slog.LogTrace("setJson", "configmanager", fmt.Sprintf("Recombining map fragments %d (%v) and %d (%v)", j, mapHistory.maps[j], j+1, mapHistory.maps[j+1]))
							mapHistory.maps[j][mapHistory.keys[j]] = mapHistory.maps[j+1]
						}
					}
					cfg.slog.LogTrace("setJson", "configmanager", fmt.Sprintf("Setting jsonConfigMap to %v", mapHistory.maps[0]))
					cfg.jsonConfigMap = mapHistory.maps[0]
					return nil
				} else {
					cfg.slog.LogTrace("setJson", "configmanager", "Result is a map, traversing it...")
					stringMap = stringMap2
				}
			}
		}
		newmap, err := cfg.setStringKey(key, value, cfg.jsonConfigMap)
		if err != nil {
			return errors.New(fmt.Sprintf("saving failed: %s", err.Error()))
		}
		cfg.jsonConfigMap = newmap
		return nil
	}
	return nil
}

// SetString sets an existing configuration key to a new string value
func (cfg *Configuration) SetString(key string, value string) (err error) {
	return cfg.setJson(key, value)
}

// Encrypt encrypts a key's string value (doesn't make sense for any other type) and replaces its unencrypted value
// in the configuration map by its encrypted value
func (cfg *Configuration) Encrypt(key string) (err error) {
	switch cfg.IsEncrypted(key) {
	case -1:
		return errors.New(fmt.Sprintf("%s does not exist", key))
	case 0:
		return errors.New(fmt.Sprintf("%s is already encrypted", key))
	}
	cfg.slog.LogTrace("Encrypt", "configmanager", fmt.Sprintf("Request to encrypt key %s received", key))
	value := cfg.GetString(key)
	if value == "" {
		return errors.New(fmt.Sprintf("%s is empty or does not exist", key))
	}
	encBytes := cfg.aesengine.Encrypt([]byte(value))
	encryptedValue := fmt.Sprintf("ENC:%s", base64.StdEncoding.EncodeToString(encBytes))
	cfg.slog.LogTrace("Encrypt", "configmanager", fmt.Sprintf("Key %s encrypted to: %s", key, encryptedValue))
	if cfg.auditing != nil {
		cfg.auditing.AuditLog(time.Now().Format("2006-01-02 15:04:05"), "configmanager", "encrypt key", fmt.Sprintf("The key %s is encrypted", key), audit.AU_SYSTEM)
	}
	return cfg.SetString(key, encryptedValue)
}

// Write writes the configuration back to a file
func (cfg Configuration) Write(filename string) (err error) {
	renameAfterWrite := false
	if filename == "" {
		filename = fmt.Sprintf("%s.new", cfg.realfilename)
		renameAfterWrite = true
	}
	filehandle, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0640)
	if err != nil {
		return err
	}
	cfg.slog.LogTrace("Write", "cfgmanager", fmt.Sprintf("About to write the following configuration map: %v", cfg.AllSettings()))
	marshaled, err := json.MarshalIndent(cfg.AllSettings(), "", "    ")
	if err != nil {
		return err
	}
	n, err := filehandle.Write(marshaled)
	if err != nil {
		return err
	}
	if n < len(marshaled) {
		return errors.New("incomplete write")
	}
	filehandle.Close()
	if renameAfterWrite {
		err = os.Remove(cfg.realfilename)
		if err != nil {
			cfg.slog.LogError("Write", "cfgmanager", fmt.Sprintf("Failed to remove old configuration file: %s", err.Error()))
		}
		err = os.Rename(filename, cfg.realfilename)
		if err != nil {
			cfg.slog.LogError("Write", "cfgmanager", fmt.Sprintf("Failed to rename temporary file to original configuration file: %s", err.Error()))
		}
	}
	if cfg.auditing != nil {
		cfg.auditing.AuditLog(time.Now().Format("2006-01-02 15:04:05"), "configmanager", "write config", fmt.Sprintf("The on-disk configuration file %s is replaced by the in-memory configuration", cfg.realfilename), audit.AU_SYSTEM)
	}
	return nil
}

func (cfg *Configuration) SetAuditing(au *audit.Audit) {
	cfg.auditing = au
}
