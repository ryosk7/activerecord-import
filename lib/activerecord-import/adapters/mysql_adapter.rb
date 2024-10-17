# frozen_string_literal: true

module ActiveRecord::Import::MysqlAdapter
  include ActiveRecord::Import::ImportSupport
  include ActiveRecord::Import::OnDuplicateKeyUpdateSupport

  NO_MAX_PACKET = 0
  QUERY_OVERHEAD = 8

  # +sql+ can be a single string or an array. If it is an array all
  # elements that are in position >= 1 will be appended to the final SQL.
  def insert_many(sql, values, options = {}, *args) # :nodoc:
    number_of_inserts = 0
    returned_values = {}
    ids = []
    results = []

    base_sql, post_sql = case sql
                         when String
                           [sql, '']
                         when Array
                           [sql.shift, sql.join(' ')]
    end

    sql_size = QUERY_OVERHEAD + base_sql.bytesize + post_sql.bytesize
    values_in_bytes = values.sum(&:bytesize)
    comma_separated_bytes = values.size - 1
    total_bytes = sql_size + values_in_bytes + comma_separated_bytes
    max = max_allowed_packet

    if max == NO_MAX_PACKET || total_bytes <= max || options[:force_single_insert]
      number_of_inserts += 1
      sql2insert = base_sql + values.join(',') + post_sql
      selections = returning_selections(options)
      
      # If selections are present, add RETURNING clause to SQL
      unless selections.blank?
        sql2insert += " RETURNING #{selections.join(', ')}"
      end
      
      returned_values = insert(sql2insert, *args)
    else
      value_sets = ::ActiveRecord::Import::ValueSetsBytesParser.parse(values, reserved_bytes: sql_size, max_bytes: max)
      transaction(requires_new: true) do
        value_sets.each do |value_set|
          number_of_inserts += 1
          sql2insert = base_sql + value_set.join(',') + post_sql
          returned_values = insert(sql2insert, *args)
        end
      end
    end

    # Handle the returning of IDs and values if RETURNING clause was used
    if options[:returning].blank?
      ids = Array(returned_values[:values])
    else
      ids, results = split_ids_and_results(returned_values, options)
    end

    ActiveRecord::Import::Result.new([], number_of_inserts, ids, results)
  end

  # Returns the maximum number of bytes that the server will allow
  def max_allowed_packet # :nodoc:
    @max_allowed_packet ||= begin
      result = execute("SELECT @@max_allowed_packet")
      val = result.respond_to?(:fetch_row) ? result.fetch_row[0] : result.first[0]
      val.to_i
    end
  end

  def pre_sql_statements(options)
    sql = []
    sql << "IGNORE" if options[:ignore] || options[:on_duplicate_key_ignore]
    sql + super
  end

  # Generate the RETURNING clause for MySQL 8.0.26+
  def returning_selections(options)
    return [] unless supports_returning?(options)

    selections = []
    column_names = Array(options[:model].column_names)

    selections += Array(options[:primary_key]) if options[:primary_key].present?
    selections += Array(options[:returning]) if options[:returning].present?

    selections.map do |selection|
      column_names.include?(selection.to_s) ? "`#{selection}`" : selection
    end
  end

  # Checks if the current MySQL version supports the RETURNING clause
  def supports_returning?(options)
    version = execute("SELECT VERSION()").first[0].split('.').map(&:to_i)
    version >= [8, 0, 26]
  end

  # Additional methods for handling the split of IDs and results
  def split_ids_and_results(returned_values, options)
    ids = []
    returning_values = []
    columns = Array(returned_values[:columns])
    values = Array(returned_values[:values])
    id_indexes = Array(options[:primary_key]).map { |key| columns.index(key) }
    returning_columns = columns.reject.with_index { |_, index| id_indexes.include?(index) }
    returning_indexes = returning_columns.map { |column| columns.index(column) }

    values.each do |value|
      value_array = Array(value)
      ids << id_indexes.map { |index| value_array[index] }
      returning_values << returning_indexes.map { |index| value_array[index] }
    end

    ids.map!(&:first) if id_indexes.size == 1
    returning_values.map!(&:first) if returning_columns.size == 1

    [ids, returning_values]
  end
end
