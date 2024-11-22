import apache_beam as beam


class ReadFromBigQuery(beam.PTransform):
    def __init__(self, query, use_standard_sql=True):
        super().__init__()
        self.query = query
        self.use_standard_sql = use_standard_sql
        self.record_counter = beam.metrics.Metrics.counter('pipeline', 'big_query_records')

    def expand(self, pcoll):
        return (
            pcoll
            | "ReadFromBigQuery" >> beam.io.ReadFromBigQuery(
                query=self.query,
                use_standard_sql=self.use_standard_sql
            )
            | "CountRecords" >> beam.Map(self._increment_counter)
        )

    def _increment_counter(self, record):
        self.record_counter.inc()
        return record
