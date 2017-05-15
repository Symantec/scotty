package cloudwatch

import (
	"fmt"
	"github.com/Symantec/scotty/chpipeline"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

type serviceClientsType struct {
	CloudWatch *cloudwatch.CloudWatch
}

func newWriter(c Config) (*Writer, error) {
	sess, err := session.NewSessionWithOptions(
		session.Options{
			Config: aws.Config{
				Credentials: credentials.NewSharedCredentials(
					c.SharedCredentialsFile, c.SharedCredentialsProfile),
				Region: aws.String(c.Region)}})
	if err != nil {
		return nil, err
	}
	serviceClientsByAccount := make(
		map[string]*serviceClientsType, len(c.Roles))
	for _, awsRole := range c.Roles {
		creds := stscreds.NewCredentials(sess, awsRole.RoleArn)
		cwService := cloudwatch.New(sess, &aws.Config{Credentials: creds})
		serviceClients := &serviceClientsType{CloudWatch: cwService}
		serviceClientsByAccount[awsRole.AccountNumber] = serviceClients
	}
	return &Writer{serviceClientsByAccount: serviceClientsByAccount}, nil
}

func (w *Writer) write(snapshot *chpipeline.Snapshot) error {
	serviceClients, ok := w.serviceClientsByAccount[snapshot.AccountNumber]
	if !ok {
		return fmt.Errorf("Unrecognizsed account: %s", snapshot.AccountNumber)
	}
	if len(snapshot.Fss) > 1 {
		return fmt.Errorf(
			"multiple file systems not supported. found %d",
			len(snapshot.Fss))
	}
	_, err := serviceClients.CloudWatch.PutMetricData(
		toPutMetricData(snapshot))
	return err
}
