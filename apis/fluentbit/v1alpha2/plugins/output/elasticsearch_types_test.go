package output

import (
	"testing"

	"github.com/fluent/fluent-operator/v3/apis/fluentbit/v1alpha2/plugins"
	"github.com/fluent/fluent-operator/v3/apis/fluentbit/v1alpha2/plugins/params"
	"github.com/fluent/fluent-operator/v3/pkg/utils"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestElasticsearch_Params(t *testing.T) {
	g := NewGomegaWithT(t)
	fcb := fake.ClientBuilder{}
	fc := fcb.WithObjects(&v1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "test_namespace", Name: "es_secret"},
		Data: map[string][]byte{
			"http_user":   []byte("expected_http_user"),
			"http_passwd": []byte("expected_http_passwd"),
		},
	}).Build()

	sl := plugins.NewSecretLoader(fc, "test_namespace")
	es := Elasticsearch{
		Host:                   "elasticsearch.example.com",
		Port:                   utils.ToPtr[int32](9200),
		Index:                  "my-index",
		Type:                   "_doc",
		BulkMessageRequestSize: "10M",
		HTTPUser:               &plugins.Secret{ValueFrom: plugins.ValueSource{SecretKeyRef: v1.SecretKeySelector{LocalObjectReference: v1.LocalObjectReference{Name: "es_secret"}, Key: "http_user"}}},
		HTTPPasswd:             &plugins.Secret{ValueFrom: plugins.ValueSource{SecretKeyRef: v1.SecretKeySelector{LocalObjectReference: v1.LocalObjectReference{Name: "es_secret"}, Key: "http_passwd"}}},
		LogstashFormat:         utils.ToPtr(true),
		LogstashPrefix:         "logstash",
		TLS:                    &plugins.TLS{Verify: utils.ToPtr(false)},
		TotalLimitSize:         "512M",
	}

	expected := params.NewKVs()
	expected.Insert("HTTP_User", "expected_http_user")
	expected.Insert("HTTP_Passwd", "expected_http_passwd")
	expected.Insert("Host", "elasticsearch.example.com")
	expected.Insert("Port", "9200")
	expected.Insert("Index", "my-index")
	expected.Insert("Type", "_doc")
	expected.Insert("Logstash_Prefix", "logstash")
	expected.Insert("Bulk_Message_Request_Size", "10M")
	expected.Insert("storage.total_limit_size", "512M")
	expected.Insert("Logstash_Format", "true")
	expected.Insert("tls", "On")
	expected.Insert("tls.verify", "false")

	kvs, err := es.Params(sl)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(kvs).To(Equal(expected))
}

func TestElasticsearch_BulkMessageRequestSize_Formats(t *testing.T) {
	g := NewGomegaWithT(t)
	sl := plugins.NewSecretLoader(nil, "test_namespace")

	testCases := []struct {
		name     string
		sizeVal  string
		expected string
	}{
		{"uppercase M", "10M", "10M"},
		{"lowercase m", "10m", "10m"},
		{"uppercase MB", "10MB", "10MB"},
		{"lowercase mb", "10mb", "10mb"},
		{"uppercase K", "512K", "512K"},
		{"lowercase k", "512k", "512k"},
		{"uppercase KB", "512KB", "512KB"},
		{"lowercase kb", "512kb", "512kb"},
		{"uppercase G", "1G", "1G"},
		{"lowercase g", "1g", "1g"},
		{"uppercase GB", "1GB", "1GB"},
		{"lowercase gb", "1gb", "1gb"},
		{"decimal with M", "1.5M", "1.5M"},
		{"decimal with MB", "2.5MB", "2.5MB"},
		{"decimal with K", "10.25K", "10.25K"},
		{"no suffix", "1048576", "1048576"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			es := Elasticsearch{
				Host:                   "elasticsearch.example.com",
				BulkMessageRequestSize: tc.sizeVal,
			}

			expected := params.NewKVs()
			expected.Insert("Host", "elasticsearch.example.com")
			expected.Insert("Bulk_Message_Request_Size", tc.expected)

			kvs, err := es.Params(sl)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(kvs).To(Equal(expected))
		})
	}
}
