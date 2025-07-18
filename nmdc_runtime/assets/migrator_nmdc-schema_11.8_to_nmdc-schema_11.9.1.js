db.jobs.updateMany(
  { "config.was_informed_by": { $type: "string" } },
  [
    {
      $set: {
        "config.was_informed_by": [ "$config.was_informed_by" ]
      }
    }
  ]
)
