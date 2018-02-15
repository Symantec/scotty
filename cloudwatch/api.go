// The cloudwatch package writes data to aws cloudwatch
package cloudwatch

import (
	"github.com/Symantec/scotty/chpipeline"
	"github.com/Symantec/scotty/lib/yamlutil"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

// AwsRole represents a single AwsRole in a config file
type AwsRole struct {

	// looks like "123456789012"
	AccountNumber string `yaml:"accountNumber"`

	// looks like "arn:aws:iam::123456789012:role/SOME-ROLE"
	RoleArn string `yaml:"roleArn"`
}

func (a *AwsRole) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type awsRoleFields AwsRole
	return yamlutil.StrictUnmarshalYAML(unmarshal, (*awsRoleFields)(a))
}

// Config represents the configuration of a Writer
type Config struct {
	Roles []AwsRole `yaml:"roles"`

	// Full path of AWS shared credentials file
	SharedCredentialsFile string `yaml:"sharedCredentialsFile"`

	// The AWS profile name to use in shared credentials file.
	// Omitting this is equivalent to specifying the "default" profile
	SharedCredentialsProfile string `yaml:"sharedCredentialsProfile"`

	// Not used anymore. Zeroed out when read.
	Region string `yaml:"region"`
}

func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type configFields Config
	err := yamlutil.StrictUnmarshalYAML(unmarshal, (*configFields)(c))
	if err != nil {
		return err
	}
	// Zero out region, we aren't using it anymore
	c.Region = ""
	return nil
}

func (c *Config) Reset() {
	*c = Config{}
}

// A Writer instance writes cloudwatch data to AWS
type Writer struct {
	credentialsByAccount map[string]*credentials.Credentials
	sess                 *session.Session
}

func NewWriter(c Config) (*Writer, error) {
	return newWriter(c)
}

// Write writes data for a single instance to AWS
func (w *Writer) Write(snapshot *chpipeline.Snapshot) error {
	return w.write(snapshot)
}
